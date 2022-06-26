package com.huifu.rtdp.sink;

import com.huifu.rtdp.kafka.dto.Metadata;
import com.huifu.rtdp.kafka.dto.MongoDataDTO;
import com.huifu.rtdp.kafka.dto.WriteModelExt;
import com.huifu.rtdp.mongodb.codec.BigDecimalCodecProvider;
import com.huifu.rtdp.mongodb.codec.BigIntegerCodecProvider;
import com.huifu.rtdp.util.MongoUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.collect.Queues;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.huifu.rtdp.util.MongoUtils.getBsonList;

/**
 * @author shuai
 */
public class MongodbSink extends RichSinkFunction<MongoDataDTO> implements CheckpointedFunction {

    private final Logger logger = LoggerFactory.getLogger(MongodbSink.class);

    public static final ReplaceOptions replaceOptions = new ReplaceOptions().bypassDocumentValidation(false).upsert(true);
    public static final UpdateOptions updateOptions = new UpdateOptions().bypassDocumentValidation(false).upsert(true);

    private final String servers;
    private final String databaseName;
    private final String username;
    private final String password;
    private final int clientThreadNum;
    private final int clientQueueCapacity;
    private final String updateMode;
    private final int batchSize;
    private final String syncCollections;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final boolean ignoreWriteError;
    private final Integer maxConnectionIdleTime;
    private final Integer maxConnectionLifeTime;
    private final Integer maxWaitTime;
    private final Integer connectTimeout;
    private final Integer connectionsPerHost;
    private final Integer socketTimeout;
    private final Boolean sslEnabled;

    private Map<String /* table */, Map<Integer /* clientId */, LinkedBlockingQueue<WriteModelExt<Document>>>> bufferQueueMap;
    private Map<String /* table */, CyclicBarrier> cyclicBarrierMap;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    private List<MultiThreadConsumerClient> consumerClients = new ArrayList<>();

    private ExecutorService executorService;

    private Map<String /*collection*/, List<String> /*key set*/> keyMap = new ConcurrentHashMap<>();

    private Map<String, MongoCollection> collectionMap = new ConcurrentHashMap<>();
    private Map<String, List<String>> collectionKeyMap = new ConcurrentHashMap<>();

    public MongodbSink(String servers, String databaseName, String username, String password,
                       int clientThreadNum, int clientQueueCapacity, String updateMode, int batchSize, String syncCollections,
                       long batchIntervalMs, int maxRetries, boolean ignoreWriteError, Boolean sslEnabled, Integer maxConnectionIdleTime,
                       Integer maxConnectionLifeTime, Integer maxWaitTime, Integer connectTimeout, int connectionsPerHost, Integer socketTimeout) {
        this.servers = servers;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;

        this.clientThreadNum = clientThreadNum;
        this.clientQueueCapacity = clientQueueCapacity;
        this.updateMode = updateMode;
        this.batchSize = batchSize;
        this.syncCollections = syncCollections;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
        this.ignoreWriteError = ignoreWriteError;

        this.sslEnabled = sslEnabled;
        this.maxConnectionIdleTime = maxConnectionIdleTime;
        this.maxConnectionLifeTime = maxConnectionLifeTime;
        this.maxWaitTime = maxWaitTime;
        this.connectTimeout = connectTimeout;
        this.connectionsPerHost = connectionsPerHost;
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.debug("打开MongodbSink");
        this.executorService = Executors.newCachedThreadPool();
        this.bufferQueueMap = new HashMap<>(8);
        this.cyclicBarrierMap = new HashMap<>(8);

        this.mongoClient = createMongoClient(servers, databaseName, username, password);
        this.mongoDatabase = this.mongoClient.getDatabase(databaseName);

        // 添加listDatabaseNames操作以解决mongodb白名单问题，期望连接不上时抛错
        List<String> databaseNames = new ArrayList<>();
        MongoCursor<String> cursor = this.mongoClient.listDatabaseNames().cursor();
        while(cursor.hasNext()) {
            databaseNames.add(cursor.next());
        }
        if (databaseNames.contains(this.databaseName)) {
            logger.info("mongodb has database: {}", this.databaseName);
        } else {
            logger.error("mongodb has NO database: {}", this.databaseName);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> MoreExecutors.shutdownAndAwaitTermination(this.executorService, 30, TimeUnit.SECONDS)));
    }

    @Override
    public void close() throws Exception {
        logger.debug("关闭MongodbSink");
        super.close();
        for (MultiThreadConsumerClient consumerClient : consumerClients) {
            consumerClient.close();
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Override
    public void invoke(MongoDataDTO tempDataDTO, Context context) throws Exception {
        String collection = tempDataDTO.getCollection();
        RowKind rowKind = tempDataDTO.getRowKind();
        List<String> keyFields = tempDataDTO.getKeyFields();
        Document doc = tempDataDTO.getNewDoc();
        Document old = tempDataDTO.getOldDoc();
        Metadata metadata = tempDataDTO.getMetadata();

        if (StringUtils.isNotBlank(syncCollections) && !syncCollections.contains(collection)) {
            // 如果设置了同步表，需要同步的表不包含这个表
            return;
        }

        Preconditions.checkNotNull(collection, "collection should not be NULL");
        MongoCollection mongoCollection = collectionMap.get(collection);
        if (mongoCollection == null) {
            mongoCollection = mongoDatabase.getCollection(collection);
            if (mongoCollection != null) {
                collectionMap.put(collection, mongoCollection);
                List<String> mongoKeys = MongoUtils.getMongoKeys(mongoCollection);
                keyMap.put(collection, mongoKeys);
                logger.info("The mongoKeys of collection `{}` are [{}]", collection, String.join(",", mongoKeys));
            }
        }

        List<String> mongoKeys = keyMap.get(collection);
        if (CollectionUtils.isNotEmpty(mongoKeys)) {
            keyFields = mongoKeys;
        }

        if (!bufferQueueMap.containsKey(collection)) {
            bufferQueueMap.put(tempDataDTO.getCollection(), new HashMap<>(clientThreadNum));
            CyclicBarrier cyclicBarrier = new CyclicBarrier(clientThreadNum + 1);
            cyclicBarrierMap.put(tempDataDTO.getCollection(), cyclicBarrier);
            // 创建并开启消费者线程
            for (int clientThreadId = 0; clientThreadId < clientThreadNum; clientThreadId++) {
                LinkedBlockingQueue<WriteModelExt<Document>> queue = Queues.newLinkedBlockingQueue(clientQueueCapacity);
                bufferQueueMap.get(tempDataDTO.getCollection()).put(clientThreadId, queue);
                MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(this, queue,
                        cyclicBarrier, mongoCollection, batchSize, batchIntervalMs, maxRetries, ignoreWriteError, replaceOptions, updateOptions);
                consumerClients.add(consumerClient);
                executorService.execute(consumerClient);
            }
        }

        // 根据主键hash值放到对应的queue中
        Map<String, Object> keyValueMap = new TreeMap<>();
        List<String> keyValueList = new ArrayList<>();
        for (String keyField : keyFields) {
            Object value = doc.get(keyField);
            keyValueList.add(value == null ? "N/A" : value.toString());
            keyValueMap.put(keyField, doc.get(keyField));
        }

        String ids = getIds(keyValueMap);
        int hash = MathUtils.murmurHash(ids.hashCode());
        Integer clientThreadId = hash % clientThreadNum;
        if (bufferQueueMap.get(collection).containsKey(clientThreadId)) {
            WriteModel<Document> writeModel = getWriteModel(rowKind, old, doc, keyValueMap, updateMode);
            if (writeModel != null) {
                // 根据主键hash值放到对应的queue中
                WriteModelExt writeModelExt = new WriteModelExt(String.join("|", keyValueList), writeModel, old, metadata);
                bufferQueueMap.get(collection).get(clientThreadId).put(writeModelExt);
            }
        } else {
            logger.error("collection: {}, data ids: {}, hash: {}, clientThreadId: {}, cannot find bufferQueue to put", tempDataDTO.getCollection(), ids, hash, clientThreadId);
        }
    }

    private MongoClient createMongoClient(String servers, String databaseName, String username, String password) {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromProviders(new BigDecimalCodecProvider()),
                CodecRegistries.fromProviders(new BigIntegerCodecProvider()),
                MongoClient.getDefaultCodecRegistry()
        );

        MongoClientOptions.Builder builder = MongoClientOptions.builder().codecRegistry(codecRegistry).retryWrites(true);

        if (maxConnectionIdleTime != null) {
            builder.maxConnectionIdleTime(maxConnectionIdleTime);
        }
        if (maxConnectionLifeTime != null) {
            builder.maxConnectionLifeTime(maxConnectionLifeTime);
        }
        if (maxWaitTime != null) {
            builder.maxWaitTime(maxWaitTime);
        }
        if (connectTimeout != null) {
            builder.connectTimeout(connectTimeout);
        }
        if (connectionsPerHost != null) {
            builder.connectionsPerHost(connectionsPerHost);
        }
        if (socketTimeout != null) {
            builder.socketTimeout(socketTimeout);
        }
        if (sslEnabled != null) {
            builder.sslEnabled(sslEnabled);
        }

        MongoClientOptions options = builder.build();

        MongoClient mongoClient = null;
        List<ServerAddress> seeds = getSeeds(servers);
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            MongoCredential credential = MongoCredential.createCredential(username, databaseName, password.toCharArray());
            mongoClient = new MongoClient(seeds, credential, options);
        } else {
            mongoClient = new MongoClient(seeds, options);
        }
        return mongoClient;
    }

    private List<ServerAddress> getSeeds(String servers) {
        List<ServerAddress> serverAddresses = new ArrayList<>();
        String[] parts = servers.split(",");
        if (parts != null && parts.length > 0) {
            for (String part : parts) {
                String[] items = part.split(":");
                if (items != null && items.length == 2) {
                    ServerAddress serverAddress = new ServerAddress(items[0], Integer.valueOf(items[1]));
                    serverAddresses.add(serverAddress);
                }
            }
        }
        return serverAddresses;
    }

    private String getIds(Map<String, Object> keyMap) {
        return String.join("|", keyMap.values().stream().map(Object::toString).collect(Collectors.toList()));
    }

    private WriteModel<Document> getWriteModel(RowKind rowKind, Document oldDoc, Document currentDoc, Map<String, Object> keyValueMap, String updateMode) {
        boolean replaceAll = updateMode.equals(MongoUtils.REPLACE_ALL);
        WriteModel<Document> writeModel = null;
        Bson filter = null;
        switch (rowKind) {
            case DELETE:
                filter = Filters.and(getBsonList(keyValueMap, oldDoc));
                writeModel = new DeleteOneModel<>(filter);
                break;
            case UPDATE_AFTER:
                if (replaceAll) {
                    filter = Filters.and(getBsonList(keyValueMap, oldDoc));
                    writeModel = new ReplaceOneModel<>(filter, currentDoc, replaceOptions);
                } else {
                    Document updateDoc = new Document();
                    updateDoc.append("$set", currentDoc);
                    filter = Filters.and(getBsonList(keyValueMap, oldDoc));
                    writeModel = new UpdateOneModel<>(filter, updateDoc, updateOptions);
                }
                break;
            case INSERT:
                if (replaceAll) {
                    filter = Filters.and(getBsonList(keyValueMap, currentDoc));
                    writeModel = new ReplaceOneModel<>(filter, currentDoc, replaceOptions);
                } else {
                    Document updateDoc = new Document();
                    updateDoc.append("$set", currentDoc);
                    filter = Filters.and(getBsonList(keyValueMap, oldDoc));
                    writeModel = new UpdateOneModel<>(filter, updateDoc, updateOptions);
                }
                break;
            default:
                break;
        }
        return writeModel;
    }

    public void updateKeys(String collectionName, List<String> keys) {
        this.keyMap.put(collectionName, keys);
    }

    public List<String> getKeys(String collectionName) {
        return this.keyMap.get(collectionName);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        logger.debug("snapshotState begin: 所有的 MultiThreadConsumerClient 准备 flush !!!");
        // barrier 开始等待
        Collection<CyclicBarrier> cyclicBarriers = cyclicBarrierMap.values();
        for (CyclicBarrier cyclicBarrier : cyclicBarriers) {
            cyclicBarrier.await();
        }
        logger.debug("snapshotState end");
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        logger.debug("initializeState: do nothing");
    }
}
