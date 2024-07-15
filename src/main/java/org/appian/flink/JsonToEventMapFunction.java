package org.appian.flink;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.appian.common.Message;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToEventMapFunction extends RichMapFunction<String,Message> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Message map(String value) throws Exception {
    return objectMapper.readValue(value, Message.class);
  }
}
