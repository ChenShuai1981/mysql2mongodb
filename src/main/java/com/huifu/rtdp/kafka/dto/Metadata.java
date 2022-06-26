package com.huifu.rtdp.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author shuai
 * @date 2022-06-26 10:29
 **/
@Data
@AllArgsConstructor
public class Metadata {
  private String topic;
  private Integer partition;
  private Long offset;
}
