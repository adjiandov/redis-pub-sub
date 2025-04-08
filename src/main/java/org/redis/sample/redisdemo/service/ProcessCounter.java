package org.redis.sample.redisdemo.service;

public interface ProcessCounter {

  void incrementCounter();

  long getCurrentCount();
}
