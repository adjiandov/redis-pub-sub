package org.redis.sample.redisdemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ConsumedMessage {

  @JsonProperty("message_id")
  private String messageId;
}
