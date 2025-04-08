package org.redis.sample.redisdemo.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redis.sample.redisdemo.model.ConsumedMessage;
import org.redis.sample.redisdemo.model.PublishedMessage;
import org.redis.sample.redisdemo.service.ProcessCounter;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class MessageConsumerTest {

  private MessageConsumer instanceUnderTest;
  private StringRedisTemplate redisTemplate;
  private ObjectMapper objectMapper;
  private ProcessCounter processCounter;

  @BeforeEach
  void init() {
    redisTemplate = mock(StringRedisTemplate.class);
    objectMapper = mock(ObjectMapper.class);
    processCounter = mock(ProcessCounter.class);

    instanceUnderTest = new MessageConsumer(redisTemplate, objectMapper, "Consumer-1", processCounter);
  }

  @Test
  void testHandleMessage_success() throws JsonProcessingException {
    final var message = "TEST_MESSAGE";
    final var objectMapperMockedReturn = new ConsumedMessage();
    objectMapperMockedReturn.setMessageId(message);
    final var valueOperations = mock(ValueOperations.class);
    when(objectMapper.readValue(message, ConsumedMessage.class)).thenReturn(objectMapperMockedReturn);
    when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    when(valueOperations.setIfAbsent(anyString(), eq("locked"), any(Duration.class)))
        .thenReturn(true);
    final var streamOperations = mock(StreamOperations.class);
    when(redisTemplate.opsForStream()).thenReturn(streamOperations);
    ArgumentCaptor<ObjectRecord> processedMessageCaptor = ArgumentCaptor.forClass(ObjectRecord.class);

    instanceUnderTest.handleMessage(message);

    verify(processCounter).incrementCounter();
    verify(streamOperations).add(processedMessageCaptor.capture());
    verify(redisTemplate).delete(anyString());
    final var publishedMessage = assertInstanceOf(PublishedMessage.class, processedMessageCaptor.getValue().getValue());
    assertNotNull(publishedMessage);
    assertEquals("Consumer-1", publishedMessage.getConsumerId());
    assertEquals("TEST_MESSAGE", publishedMessage.getMessageId());
  }

  @Test
  void testHandleMessage_lockNotAcquired() throws JsonProcessingException {
    final var message = "TEST_MESSAGE";
    final var objectMapperMockedReturn = new ConsumedMessage();
    objectMapperMockedReturn.setMessageId(message);
    final var valueOperations = mock(ValueOperations.class);
    when(objectMapper.readValue(message, ConsumedMessage.class)).thenReturn(objectMapperMockedReturn);
    when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    when(valueOperations.setIfAbsent(anyString(), eq("locked"), any(Duration.class)))
        .thenReturn(false);

    instanceUnderTest.handleMessage(message);

    verify(processCounter, never()).incrementCounter();
    verify(redisTemplate, never()).delete(anyString());
  }

  @Test
  void testHandleMessage_throwsJsonParsingException() throws JsonProcessingException {
    final var message = "TEST_MESSAGE";
    final var objectMapperMockedReturn = new ConsumedMessage();
    objectMapperMockedReturn.setMessageId(message);
    final var processingException = mock(JsonProcessingException.class);
    when(objectMapper.readValue(message, ConsumedMessage.class)).thenThrow(processingException);

    final var result = assertThrows(RuntimeException.class, () -> instanceUnderTest.handleMessage(message));

    assertNotNull(result);
    verify(processCounter, never()).incrementCounter();
    verify(redisTemplate, never()).delete(anyString());
    verify(redisTemplate, never()).opsForValue();
  }
}
