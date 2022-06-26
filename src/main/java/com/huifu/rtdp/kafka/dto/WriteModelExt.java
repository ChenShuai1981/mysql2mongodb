package com.huifu.rtdp.kafka.dto;

import com.mongodb.client.model.WriteModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.bson.conversions.Bson;

/**
 * @author shuai
 * @date 2021-11-24 13:36
 **/
@Data
@AllArgsConstructor
public class WriteModelExt<T> {
  private String keyString;
  private WriteModel<T> writeModel;
  private Bson old;
  private Metadata metaData;

//  @Override
//  public boolean equals(Object obj) {
//    if (obj == null) {
//      return false;
//    }
//    if (!(obj instanceof WriteModelExt)) {
//      return false;
//    }
//    WriteModelExt a = (WriteModelExt) obj;
//      return a.keyString == this.keyString;
//  }
//
//  @Override
//  public int hashCode() {
//    return this.keyString.hashCode();
//  }
}
