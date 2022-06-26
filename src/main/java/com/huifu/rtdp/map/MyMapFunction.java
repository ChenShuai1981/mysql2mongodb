package com.huifu.rtdp.map;

import com.huifu.rtdp.kafka.dto.Metadata;
import com.huifu.rtdp.kafka.dto.MongoDataDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.RowKind;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huifu.rtdp.util.SqlTypeUtils.transformToSpecificType;

/**
 * @author shuai
 * @date 2021-12-29 13:47
 **/
public class MyMapFunction implements MapFunction<ObjectNode, MongoDataDTO> {

  private static Logger logger = LoggerFactory.getLogger(MyMapFunction.class);

  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public MongoDataDTO map(ObjectNode objectNode) throws Exception {
    // 输出消息内容
//    logger.debug("MyMapFunction => " + objectNode.toString());

    String collection = objectNode.get("value").get("table").textValue();
    ArrayNode pkNamesArrayNode = (ArrayNode) objectNode.get("value").get("pkNames");
    List<String> keyFields = new ArrayList<>();
    if (pkNamesArrayNode != null && pkNamesArrayNode.size() > 0) {
      for (int i=0; i<pkNamesArrayNode.size(); i++) {
        keyFields.add(pkNamesArrayNode.get(i).textValue());
      }
    }
    Map<String, Integer> sqlTypeMap = mapper.convertValue(
            objectNode.get("value").get("sqlType"),
            new TypeReference<Map<String, Integer>>(){});

    ArrayNode dataArrayNode = (ArrayNode) objectNode.get("value").get("data");
    ArrayNode oldArrayNode = (ArrayNode) objectNode.get("value").get("old");
    Map<String, Object> dataMap = new HashMap<>(8);
    if (dataArrayNode != null && dataArrayNode.size() > 0) {
      Map<String, Object> canalDataMap = mapper.convertValue(dataArrayNode.get(0), new TypeReference<Map<String, Object>>(){});
      dataMap = transformToSpecificType(sqlTypeMap, canalDataMap);
    }
    Map<String, Object> oldMap = new HashMap<>(8);
    if (oldArrayNode != null && oldArrayNode.size() > 0) {
      Map<String, Object> canalOldMap = mapper.convertValue(oldArrayNode.get(0), new TypeReference<Map<String, Object>>(){});
      oldMap = transformToSpecificType(sqlTypeMap, canalOldMap);
    }
    Document doc = new Document(dataMap);
    Document old = new Document(oldMap);

    String type = objectNode.get("value").get("type").textValue();
    RowKind rowKind = null;
    switch (type) {
      case "UPDATE":
        // old是更新前的数据，data是更新后的数据
        rowKind = RowKind.UPDATE_AFTER;
        break;
      case "INSERT":
        // data是待插入的数据
        rowKind = RowKind.INSERT;
        break;
      case "DELETE":
        // data是待删除的数据
        rowKind = RowKind.DELETE;
        break;
      default:
        logger.error("invalid operation type, supported UPSERT, INSERT, DELETE only, but it is " + type);
        return null;
    }

    JsonNode metadataNode = objectNode.get("metadata");
    String topic = metadataNode.get("topic").asText();
    Integer partition = metadataNode.get("partition").asInt();
    Long offset = metadataNode.get("offset").asLong();
    Metadata metadata = new Metadata(topic, partition, offset);

    MongoDataDTO tempDataDTO = new MongoDataDTO(rowKind, collection, old, doc, keyFields, metadata);
    return tempDataDTO;
  }

}
