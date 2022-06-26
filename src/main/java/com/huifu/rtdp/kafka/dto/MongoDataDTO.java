package com.huifu.rtdp.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.types.RowKind;
import org.bson.Document;

import java.io.Serializable;
import java.util.List;

/**
 * @author shuai
 */
@Data
@AllArgsConstructor
public class MongoDataDTO implements Serializable {
  private RowKind rowKind;
  private String collection;
  private Document oldDoc;
  private Document newDoc;
  private List<String> keyFields;
  private Metadata metadata;
}
