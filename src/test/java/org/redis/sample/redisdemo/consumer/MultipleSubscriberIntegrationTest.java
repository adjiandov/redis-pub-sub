package org.redis.sample.redisdemo.consumer;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.redis.sample.redisdemo.config.RedisPubSubConfig;
import org.redis.sample.redisdemo.service.ProcessCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@ComponentScan(excludeFilters = {@ComponentScan.Filter(RedisPubSubConfig.class) })
class MultipleSubscriberIntegrationTest {

  @Container
  static GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse("redis:7.0.10-alpine")).withExposedPorts(6379);

  @Autowired
  private StringRedisTemplate redisTemplate;
  @Autowired
  private ProcessCounter processCounter;
  @Value("${redis.pubsub.channel}")
  private String channelName;


  @DynamicPropertySource
  static void overrideRedisProps(DynamicPropertyRegistry registry) {
    REDIS_CONTAINER.start();
    registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
    registry.add("spring.data.redis.port", () -> REDIS_CONTAINER.getFirstMappedPort());
  }

  @Test
  void testPublishMessageAndOnlyOneSubscriberProcessesIt() throws Exception {
    redisTemplate.convertAndSend(channelName, "{\"message_id\": \"test-message-1\"}");

    // Wait a bit for consumers to pick it up
    sleep(2000);

    assertEquals(1, processCounter.getCurrentCount(),
        "Exactly one subscriber should have processed the message");
  }
}
