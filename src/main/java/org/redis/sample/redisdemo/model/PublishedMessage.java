package org.redis.sample.redisdemo.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PublishedMessage {
  private String messageId;
  private String consumerId;
  private String timestamp;
}
