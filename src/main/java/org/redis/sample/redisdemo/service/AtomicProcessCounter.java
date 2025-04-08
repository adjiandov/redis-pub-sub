package org.redis.sample.redisdemo.service;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

@Component
public class AtomicProcessCounter implements ProcessCounter {
  private final AtomicLong processCounter = new AtomicLong(0);

  @Override
  public void incrementCounter() {
    processCounter.incrementAndGet();
  }

  @Override
  public long getCurrentCount() {
    return processCounter.get();
  }
}
