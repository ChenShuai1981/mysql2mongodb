package com.huifu.rtdp;

import com.huifu.rtdp.kafka.deserialization.schema.JSONKeyValueDeserializationSchema;
import com.huifu.rtdp.map.MyMapFunction;
import com.huifu.rtdp.util.MongoUtils;
import com.huifu.rtdp.sink.MongodbSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *  Mysql数据同步到Mongodb入口类
 *  必填参数
 *
 *  选填参数
 *
 * @author shuai
 */
public class Mysql2Mongo {

    private static Logger logger = LoggerFactory.getLogger(Mysql2Mongo.class);

    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        String kafkaServers = params.getRequired("kafka-servers");
        String topicName = params.getRequired("kafka-topic");
        List<String> topicNames = Arrays.asList(topicName.split(","));
        String groupId = params.getRequired("kafka-topic-group");
        String offsetReset = params.get("kafka-offset-reset", "earliest");
        String startFrom = params.get("start-from", "earliest");
        Integer startupBackfillMinutes = params.getInt("startup-backfill-minutes", 10);
        Integer maxPollRecords = params.getInt("max-poll-records", 500);
        Integer maxPartitionFetchBytes = params.getInt("max-partition-fetch-bytes", 1048576);
        Integer fetchMaxBytes = params.getInt("fetch.max.bytes", 52428800);

        String servers = params.getRequired("mongo-servers");
        String databaseName = params.getRequired("mongo-database");
        String username = params.get("mongo-username");
        String password = params.get("mongo-password");

        String startupOffsets = params.get("startup-offsets", null);
        String syncCollections = params.get("sync-collections", null);
        Long startupTimestamp = params.getLong("startup-timestamp", -1);
        String updateMode = params.get("update-mode", MongoUtils.REPLACE_ALL);
        int clientThreadNum = params.getInt("client-thread-number", 1);
        int clientQueueCapacity = params.getInt("client-queue-capacity", 1000);
        int batchSize = params.getInt("batch-size", 10);
        long batchIntervalMs = params.getLong("batch-interval-ms", -1);
        int maxRetries = params.getInt("max-retries", 3);
        boolean ignoreWriteError = params.getBoolean("ignore-write-error", false);
        Boolean sslEnabled = params.getBoolean("ssl-enabled", false);
        Integer maxConnectionIdleTime = params.getInt("max-connection-idle-time", 3000);
        Integer maxConnectionLifeTime = params.getInt("max-connection-life-time", 5000);
        Integer maxWaitTime = params.getInt("max-wait-time", 120000);
        Integer connectTimeout = params.getInt("connection-timeout", 10000);
        int connectionsPerHost = params.getInt("connections-per-host", 50);
        Integer socketTimeout = params.getInt("socket-timeout", 0);

        long partitionDiscoveryIntervalMs = params.getLong("partition-discovery-interval", 30 * 60 * 1000);
        long checkpointInterval = params.getLong("checkpoint-interval", 10 * 1000);
        long checkpointTimeout = params.getInt("checkpoint-timeout", 60 * 1000);
        int maxConcurrentCheckpoints = params.getInt("max-concurrent-checkpoints", 1);
        int tolerableCheckpointFailureNum = params.getInt("tolerable-checkpoint-failure-num", 2);

        int mapParallelism = params.getInt("map-parallelism", -1);
        int sinkParallelism = params.getInt("sink-parallelism", -1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        /**
         * 设置checkpint
         */
        //每分钟 开始一次 checkpoint
        env.enableCheckpointing(checkpointInterval);
        //精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        //允许出现的checkpoint错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(tolerableCheckpointFailureNum);
        //checkpoint的超时时间，单位毫秒
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

        int parallelism = env.getParallelism();
        if (mapParallelism < 0) {
            mapParallelism = parallelism;
        }
        if (sinkParallelism < 0) {
            sinkParallelism = parallelism;
        }

        Properties properties = new Properties();
        // 禁止自动提交offset
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put("bootstrap.servers", kafkaServers);
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", offsetReset);
        properties.put("max.partition.fetch.bytes", maxPartitionFetchBytes); // 1048576 (1MB)
        properties.put("max.poll.records", maxPollRecords); // 500
        properties.put("fetch.max.bytes", fetchMaxBytes); // 52428800 (50MB)
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000"); // 默认30秒
        properties.setProperty("flink.partition-discovery.interval-millis", String.valueOf(partitionDiscoveryIntervalMs));

        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>(topicNames, new JSONKeyValueDeserializationSchema(true), properties);
        consumer.setCommitOffsetsOnCheckpoints(true);
        startConsumer(startFrom, startupBackfillMinutes, startupOffsets, startupTimestamp, consumer);

        DataStream<ObjectNode> objectNodeStream = env
                .addSource(consumer)
                .filter(Objects::nonNull)
                .uid("kafka_consumer")
                .name("kafka_consumer");

        objectNodeStream.map(new MyMapFunction()).setParallelism(mapParallelism).uid("toMongoData").name("toMongoData")
        .addSink(new MongodbSink(servers, databaseName, username, password, clientThreadNum,
                clientQueueCapacity, updateMode, batchSize, syncCollections, batchIntervalMs, maxRetries,
                ignoreWriteError, sslEnabled, maxConnectionIdleTime, maxConnectionLifeTime,
                maxWaitTime, connectTimeout, connectionsPerHost, socketTimeout)
        ).setParallelism(sinkParallelism).uid("mongodb_sink").name("mongodb_sink");

        //执行Job
        env.execute();
    }

    private static void startConsumer(String startFrom, Integer startupBackfillMinutes, String startupOffsets, Long startupTimestamp, FlinkKafkaConsumer<ObjectNode> consumer) {
        switch (startFrom) {
            case "backfill":
                Long startFromTimestamp = System.currentTimeMillis() - startupBackfillMinutes * 60 * 1000;
                consumer.setStartFromTimestamp(startFromTimestamp);
                break;
            case "earliest":
                consumer.setStartFromEarliest();
                break;
            case "specificStartupTimestamp":
                if (startupTimestamp == -1) {
                    throw new IllegalArgumentException("You should provide `startup-timestamp` parameter value when use `specificStartupTimestamp` of `startFrom` parameter");
                }
                consumer.setStartFromTimestamp(startupTimestamp);
                break;
            case "specificStartupOffsets":
                Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
                if (StringUtils.isBlank(startupOffsets)) {
                    throw new IllegalArgumentException("You should provide `startup-offsets` parameter value in `topic1:aa,partition:0,offset:12;topic1:aa,partition:1,offset:11;...` format when use `specificStartupOffsets` of `startFrom` parameter");
                }
                String[] items = startupOffsets.split(";");
                for (String item : items) {
                    String[] parts = item.split(",");
                    String topic = null;
                    Integer partition = null;
                    Long offset = null;
                    for (String part : parts) {
                        String[] pair = part.split(":");
                        switch (pair[0]) {
                            case "topic":
                                topic = pair[1];
                                break;
                            case "partition":
                                partition = Integer.valueOf(pair[1]);
                                break;
                            case "offset":
                                offset = Long.valueOf(pair[1]);
                                break;
                        }
                    }
                    specificStartupOffsets.put(new KafkaTopicPartition(topic, partition), offset);
                }
                consumer.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case "groupOffsets":
                consumer.setStartFromGroupOffsets();
                break;
            case "latest":
            default:
                consumer.setStartFromLatest();
                break;
        }
    }

}
