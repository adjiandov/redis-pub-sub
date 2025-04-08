package org.redis.sample.redisdemo.config;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.redis.sample.redisdemo.consumer.MessageConsumer;
import org.redis.sample.redisdemo.service.ProcessCounter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

@Configuration
public class RedisPubSubConfig {

  @Value("${redis.pubsub.channel}")
  private String channelName;
  @Value("${redis.pubsub.consumer-count}")
  private int consumerCount;

  @Bean
  public RedisMessageListenerContainer redisMessageListenerContainer(
      final RedisConnectionFactory redisConnectionFactory,
      final StringRedisTemplate redisTemplate,
      final ObjectMapper objectMapper,
      final ProcessCounter processCounter) {

    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(redisConnectionFactory);
    for (int i = 0; i < consumerCount; i++) {
      MessageConsumer handler = new MessageConsumer(redisTemplate, objectMapper, "Consumer-"+i, processCounter);
      final var listener = new MessageListenerAdapter(handler, "handleMessage");
      listener.afterPropertiesSet();
      container.addMessageListener(
          listener,
          new PatternTopic(channelName)
      );
    }
    ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 0, MILLISECONDS, new LinkedBlockingQueue<>());

    container.setTaskExecutor(executor);
    return container;
  }
}

