package org.redis.sample.redisdemo.consumer;

import static java.lang.Boolean.TRUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redis.sample.redisdemo.model.ConsumedMessage;
import org.redis.sample.redisdemo.model.PublishedMessage;
import org.redis.sample.redisdemo.service.ProcessCounter;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

@Slf4j
@RequiredArgsConstructor
public class MessageConsumer {

  private static final long LOCK_TTL_SECONDS = 30;
  public static final String REDIS_CONSUMER_LIST_KEY = "consumer:ids";
  public static final String LOCK_KEY_PREFIX = "lock:";
  public static final String LOCK_VALUE = "locked";

  private final StringRedisTemplate redisTemplate;
  private final ObjectMapper objectMapper;
  private final String consumerId;
  private final ProcessCounter processCounter;

  public void handleMessage(String rawMessage) {
    try {
      ConsumedMessage consumedMessage = objectMapper.readValue(rawMessage, ConsumedMessage.class);
      String lockKey = LOCK_KEY_PREFIX + consumedMessage.getMessageId();

      boolean acquired = acquireLock(lockKey, LOCK_TTL_SECONDS);
      if (acquired) {
        try {
          processMessage(consumedMessage);
        } finally {
          releaseLock(lockKey);
        }
      } else {
         log.info("Lock not acquired");
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @PostConstruct
  public void init() {
    redisTemplate.opsForList().rightPush(REDIS_CONSUMER_LIST_KEY, consumerId);
  }

  @PreDestroy
  public void cleanup() {
    redisTemplate.opsForList().remove(REDIS_CONSUMER_LIST_KEY, 1, consumerId);
  }

  private boolean acquireLock(String lockKey, long ttlInSeconds) {
    Boolean success = redisTemplate.opsForValue().setIfAbsent(
        lockKey,
        LOCK_VALUE,
        Duration.ofSeconds(ttlInSeconds)
    );
    return TRUE.equals(success);
  }

  private void releaseLock(String lockKey) {
    redisTemplate.delete(lockKey);
  }

  private void processMessage(ConsumedMessage consumedMessage) {
    final var publishedMessage = PublishedMessage.builder()
        .messageId(consumedMessage.getMessageId())
        .consumerId(consumerId)
        .timestamp(OffsetDateTime.now().toString())
        .build();
    processCounter.incrementCounter();
    redisTemplate.opsForStream().add(
        StreamRecords.objectBacked(publishedMessage)
            .withStreamKey("messages:processed")
    );
  }
}

