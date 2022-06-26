package com.huifu.rtdp.util;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author shuai
 * @date 2021-12-15 10:04
 **/
public class MongoUtils {

  private static final Logger logger = LoggerFactory.getLogger(MongoUtils.class);

  public static final String REPLACE_ALL = "replace-all";
  public static final String UPDATE_PARTIAL = "update-partial";

  public static List<String> getMongoKeys(MongoCollection mongoCollection) {
    List<String> keys = new ArrayList<>();
    if (mongoCollection != null) {
      ListIndexesIterable<Document> listIndexesIterable =  mongoCollection.listIndexes();
      if (listIndexesIterable != null) {
        MongoCursor<Document> indexIt = null;
        try {
          indexIt = listIndexesIterable.iterator();
          while (indexIt.hasNext()) {
            Document index = indexIt.next();
            boolean unique = false;
            if (index.containsKey("unique") && (Boolean) index.get("unique")) {
              unique = true;
            }
            Object keyValue = index.get("key");
            if (keyValue instanceof Document) {
              Document kv = (Document) keyValue;
              for (Map.Entry<String, Object> entry : kv.entrySet()) {
                if (unique) {
                  // unique key
                  keys.add(entry.getKey());
                }
                Object entryValue = entry.getValue();
                if (entryValue instanceof String && "hashed".equals(entryValue)) {
                  // shard key
                  keys.add(entry.getKey());
                }
              }
            }
          }
        } catch (Exception e) {
          logger.error("Error getKeys of collection: " + mongoCollection.getNamespace().getCollectionName(), e);
        }
      }
    }
    return keys;
  }

  public static List<Bson> getBsonList(Map<String, Object> keyMap, Bson old) {
    List<Bson> bsonList = new ArrayList<>();
    for (Map.Entry<String, Object> entry : keyMap.entrySet()) {
      bsonList.add(Filters.eq(entry.getKey(), entry.getValue()));
    }
    if (bsonList.isEmpty()) {
      // 针对无主键表只能进行全字段过滤
      bsonList.add(old);
    }
    return bsonList;
  }
}
