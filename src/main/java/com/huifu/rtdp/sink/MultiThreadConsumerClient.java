package com.huifu.rtdp.sink;

import com.huifu.rtdp.kafka.dto.Metadata;
import com.huifu.rtdp.kafka.dto.WriteModelExt;
import com.huifu.rtdp.util.MongoUtils;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.huifu.rtdp.util.MongoUtils.getBsonList;

/**
 * mongodb 多线程sink
 * 通过设置 clientThreadNum 启动多个 consumerClient 消费各自队列中的数据，来实现提高sink并发度的目标
 *
 * @author shuai
 * @date 2021-05-26 17:41
 **/
public class MultiThreadConsumerClient implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadConsumerClient.class);

    private final LinkedBlockingQueue<WriteModelExt<Document>> queue;
    private final CyclicBarrier cyclicBarrier;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final boolean ignoreWriteError;
    private final ReplaceOptions replaceOptions;
    private final UpdateOptions updateOptions;
    private final MongoCollection mongoCollection;
    private final List<WriteModelExt<Document>> writeModelExtList = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final transient MongodbSink mongodbSink;

    private final transient BulkWriteOptions bulkWriteOptions;

    private transient volatile Exception flushException;
    private transient volatile boolean closed = false;

    private static final String MONGO_ID_FIELD = "_id";

    public MultiThreadConsumerClient(MongodbSink mongodbSink,
                                     LinkedBlockingQueue<WriteModelExt<Document>> queue,
                                     CyclicBarrier cyclicBarrier,
                                     MongoCollection mongoCollection,
                                     int batchSize,
                                     long batchIntervalMs,
                                     int maxRetries,
                                     boolean ignoreWriteError,
                                     ReplaceOptions replaceOptions,
                                     UpdateOptions updateOptions) {
        this.mongodbSink = mongodbSink;
        this.queue = queue;
        this.cyclicBarrier = cyclicBarrier;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
        this.ignoreWriteError = ignoreWriteError;
        this.mongoCollection = mongoCollection;
        this.bulkWriteOptions = new BulkWriteOptions().ordered(false).bypassDocumentValidation(false);
        this.replaceOptions = replaceOptions;
        this.updateOptions = updateOptions;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        if (this.batchIntervalMs > 0) {
            // 随机10秒
            Random r = new Random();
            long delay = this.batchIntervalMs + r.nextInt(10000);
            this.scheduler.schedule(() -> {
                if (CollectionUtils.isNotEmpty(writeModelExtList)) {
                    try {
                        flush();
                    } catch (Exception e) {
                        LOGGER.error("MultiThreadConsumerClient run error!", e);
                        this.flushException = e;
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void checkFlushException() {
        if (this.flushException != null) {
            throw new RuntimeException("Flush records into mongodb failed.", this.flushException);
        }
    }

    @Override
    public void run() {
        try {
            WriteModelExt<Document> writeModelExt = null;
            while (!closed) {
                checkFlushException();
                // 从 bufferQueue 的队首消费数据，并设置 timeout
                writeModelExt = queue.poll(50, TimeUnit.MILLISECONDS);
                // writeModel != null 表示 bufferQueue 有数据
                if (writeModelExt != null) {
                    writeModelExtList.add(writeModelExt);
                    // 超过batchSize时或者发生checkpoint时会执行flush操作，将数据bulkWrite进mongodb
                    if (writeModelExtList.size() >= batchSize) {
                        flush();
                    }
                } else {
                    // entity == null 表示 bufferQueue 中已经没有数据了，
                    // 且 barrier wait 大于 0 表示当前正在执行 Checkpoint，
                    // client 需要执行 flush，保证 Checkpoint 之前的数据都消费完成
                    if (cyclicBarrier.getNumberWaiting() > 0) {
                        LOGGER.info("checkpoint flush, " +  "当前 wait 的线程数：" + cyclicBarrier.getNumberWaiting());
                        // client 执行 flush 操作，防止丢数据
                        flush();
                        cyclicBarrier.await();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("MultiThreadConsumerClient run error!", e);
            this.flushException = e;
        }
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        // 重试
        for (int i = 0; i <= maxRetries; i++) {
            try {
                attemptFlush();
                break;
            } catch (SQLException e) {
                if (i >= maxRetries) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(100 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    public void attemptFlush() throws SQLException {
        if (CollectionUtils.isNotEmpty(writeModelExtList)) {
            Map<String, WriteModelExt<Document>> map = new HashMap<>();
            for (WriteModelExt<Document> writeModelExt : writeModelExtList) {
                map.put(writeModelExt.getKeyString(), writeModelExt);
            }

            Collection<WriteModelExt<Document>> writeModelExtCollection = map.values();
            if (LOGGER.isDebugEnabled()) {
                for (WriteModelExt<Document> writeModelExt : writeModelExtCollection) {
                    Metadata metadata = writeModelExt.getMetaData();
                    LOGGER.debug("attemptFlushWriteModel -> topic: {}, partition: {}, offset: {}", metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                }
            }

            List<WriteModel<Document>> writeModels = writeModelExtCollection.stream().map(WriteModelExt::getWriteModel).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(writeModels)) {
                return;
            }
            try {
                BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(writeModels, bulkWriteOptions);
                int insertCount = bulkWriteResult.getInsertedCount();
                int deletedCount = bulkWriteResult.getDeletedCount();
                int modifiedCount = bulkWriteResult.getModifiedCount();
                int matchedCount = bulkWriteResult.getMatchedCount();
                LOGGER.info("writeModelsSize: {}, insertCount: {}, deletedCount: {}, modifiedCount: {}, matchedCount: {}", writeModels.size(), insertCount, deletedCount, modifiedCount, matchedCount);
            } catch (MongoBulkWriteException e) {
                LOGGER.warn("1st MongoBulkWriteException", e);
                List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();
                boolean hasExtractShardKeyError = false;
                for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                    String bulkWriteErrorMsg = bulkWriteError.toString();
                    hasExtractShardKeyError = hasExtractShardKeyError || bulkWriteErrorMsg.contains("could not extract exact shard key");
                }
                if (!ignoreWriteError) {
                    List<String> newKeys = new ArrayList<>();
                    String collectionName = mongoCollection.getNamespace().getCollectionName();
                    if (hasExtractShardKeyError) {
                        LOGGER.info("更新shard keys: {}", collectionName);
                        newKeys = MongoUtils.getMongoKeys(mongoCollection);
                        mongodbSink.updateKeys(collectionName, newKeys);
                    }
                    for (WriteModelExt<Document> writeModelExt : writeModelExtList) {
                        WriteModel<Document> writeModel = writeModelExt.getWriteModel();
                        Bson old = writeModelExt.getOld();
                        if (writeModel instanceof InsertOneModel) {
                            handleInsertError(collectionName, (InsertOneModel<Document>) writeModel, old);
                        } else {
                            // 除了InsertOneModel外
                            if (hasExtractShardKeyError) {
                                // 如果 shard key 出错
                                if (writeModel instanceof DeleteOneModel) {
                                    handleDeleteError(newKeys, writeModel, old);
                                } else if (writeModel instanceof ReplaceOneModel) {
                                    handleReplaceError(newKeys, writeModel, old);
                                } else if (writeModel instanceof UpdateOneModel) {
                                    handleUpdateError(newKeys, writeModel, old);
                                }
                            } else {
                                // 非 shard key 出错
                                try {
                                    mongoCollection.bulkWrite(Arrays.asList(writeModel), bulkWriteOptions);
                                } catch (MongoBulkWriteException e2) {
                                    LOGGER.error("single write error: {}", writeModel);
                                    bulkWriteErrors = e2.getWriteErrors();
                                    for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                                        LOGGER.error("非shard key出错后写单条记录失败! {}", bulkWriteError);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Bulk write mongodb error.", e);
                for (WriteModelExt writeModelExt : writeModelExtList) {
                    LOGGER.error(writeModelExt.toString());
                }
                this.flushException = e;
            } finally {
                writeModelExtList.clear();
            }
        }
    }

    private void handleUpdateError(List<String> newKeys, WriteModel<Document> writeModel, Bson old) {
        final UpdateOneModel<Document> updateOneModel = (UpdateOneModel<Document>) writeModel;
        Document update = (Document) updateOneModel.getUpdate();
        Map newKeyMap = newKeys.stream().collect(Collectors.toMap(Function.identity(), key -> update.get(key)));
        UpdateOneModel newUpdateOneModel = new UpdateOneModel(Filters.and(getBsonList(newKeyMap, old)), update, updateOptions);
        try {
            mongoCollection.bulkWrite(Arrays.asList(newUpdateOneModel), bulkWriteOptions);
        } catch (MongoBulkWriteException e) {
            LOGGER.error("single update error: {}", newUpdateOneModel);
            List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                LOGGER.error("shard key出错后Update单条记录失败! {}", bulkWriteError);
            }
        }
    }

    private void handleReplaceError(List<String> newKeys, WriteModel<Document> writeModel, Bson old) {
        final ReplaceOneModel<Document> replaceOneModel = (ReplaceOneModel<Document>) writeModel;
        Document replacement = replaceOneModel.getReplacement();
        Map newKeyMap = newKeys.stream().collect(Collectors.toMap(Function.identity(), key -> replacement.get(key)));
        ReplaceOneModel newReplaceOneModel = new ReplaceOneModel(Filters.and(getBsonList(newKeyMap, old)), replacement, replaceOptions);
        try {
            mongoCollection.bulkWrite(Arrays.asList(newReplaceOneModel), bulkWriteOptions);
        } catch (MongoBulkWriteException e) {
            LOGGER.error("single replace error: {}", newReplaceOneModel);
            List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                LOGGER.error("shard key出错后Replace单条记录失败! {}", bulkWriteError);
            }
        }
    }

    private void handleDeleteError(List<String> newKeys, WriteModel writeModel, Bson old) {
        final DeleteOneModel deleteOneModel = (DeleteOneModel) writeModel;
        Bson filter = deleteOneModel.getFilter();
        Document found = (Document) mongoCollection.find(filter).first();
        Map newKeyMap = newKeys.stream().collect(Collectors.toMap(Function.identity(), key -> found.get(key)));
        DeleteOneModel newDeleteOneModel = new DeleteOneModel<>(Filters.and(getBsonList(newKeyMap, old)));
        try {
            mongoCollection.bulkWrite(Arrays.asList(newDeleteOneModel), bulkWriteOptions);
        } catch (MongoBulkWriteException mongoBulkWriteException) {
            LOGGER.error("single delete error: {}", newDeleteOneModel);
            List<BulkWriteError> bulkWriteErrors = mongoBulkWriteException.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                LOGGER.error("shard key出错后Delete单条记录失败! {}", bulkWriteError);
            }
        }
    }

    private void handleInsertError(String collectionName, WriteModel<Document> writeModel, Bson old) {
        final InsertOneModel<Document> insertOneModel = (InsertOneModel<Document>) writeModel;
        Document doc = insertOneModel.getDocument();
        if (doc.containsKey(MONGO_ID_FIELD)) {
            doc.remove(MONGO_ID_FIELD);
        }
        Map newKeyMap = mongodbSink.getKeys(collectionName).stream().collect(Collectors.toMap(Function.identity(), shardKey -> doc.get(shardKey)));
        WriteModel<Document> newReplaceOneModel = new ReplaceOneModel<>(Filters.and(getBsonList(newKeyMap, old)), doc, replaceOptions);
        try {
            mongoCollection.bulkWrite(Arrays.asList(newReplaceOneModel), bulkWriteOptions);
        } catch (MongoBulkWriteException e1) {
            LOGGER.error("single insert->replace error: {}", newReplaceOneModel);
            List<BulkWriteError> bulkWriteErrors = e1.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                String errMsg = bulkWriteError.toString();
                if (errMsg.contains("duplicate key error")) {
                    // sharding collection replace的话仍会抛duplicate key error，故忽略duplicate key
                    LOGGER.warn("当改插入为替换后忽略duplicate key error: " + errMsg);
                } else {
                    try {
                        mongoCollection.bulkWrite(Arrays.asList(insertOneModel), bulkWriteOptions);
                    } catch (MongoBulkWriteException e2) {
                        LOGGER.error("single insert error: {}", insertOneModel);
                        bulkWriteErrors = e2.getWriteErrors();
                        for (BulkWriteError bwe : bulkWriteErrors) {
                            LOGGER.error("非dup key error后写单条记录失败! {}", bwe);
                        }
                    }
                }
            }
        }
    }

    public void close() {
        if (!closed) {
            closed = true;
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (CollectionUtils.isNotEmpty(writeModelExtList)) {
                try {
                    flush();
                } catch (Exception e) {
                    LOGGER.warn("Writing records to Mongodb failed.", e);
                    this.flushException = e;
                    throw new RuntimeException("Writing records to Mongodb failed.", e);
                }
            }
        }
        checkFlushException();
    }

}
