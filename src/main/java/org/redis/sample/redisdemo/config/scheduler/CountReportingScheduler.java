package org.redis.sample.redisdemo.config.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redis.sample.redisdemo.service.ProcessCounter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CountReportingScheduler {
  private final ProcessCounter processCounter;
  private final AtomicLong reportCount = new AtomicLong(0);
  @Value("${message.processing.reporting.rate.ms:3000}")
  private int reportingRate;


  @Scheduled(fixedRateString = "${message.processing.reporting.rate.ms:3000}")
  public void reportProcessRate() {
    final var lastReportedCount = reportCount.get();
    final var currentProcessedCount = processCounter.getCurrentCount();
    final var diff = currentProcessedCount - lastReportedCount;
    final var reportingRateSeconds = reportingRate / 1000;

    final var processRate = diff / reportingRateSeconds;

    if (diff > 0) {
      log.info("There have been {} messages processed in the last {} seconds. Process rate {} msg/s", diff,
          reportingRateSeconds, processRate);
    } else {
      log.info("No messages processed in the last {} seconds.", reportingRateSeconds);
    }
    reportCount.set(currentProcessedCount);
  }
}
