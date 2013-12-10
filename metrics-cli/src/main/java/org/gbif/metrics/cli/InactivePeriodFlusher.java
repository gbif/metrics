package org.gbif.metrics.cli;

import org.gbif.metrics.cube.HBaseCubes;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.urbanairship.datacube.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that flushes on periods of inactivity.
 * Clients should run a thread with this class , and then reset as messages are received to stop
 * the auto flush on inactive periods.
 */
class InactivePeriodFlusher implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(InactivePeriodFlusher.class);
  private static final long INACTIVITY_FLUSH_INTERVAL_MSECS = TimeUnit.SECONDS.toMillis(10);
  private static final long SLEEP_TIME_MSECS = TimeUnit.SECONDS.toMillis(1);

  private final Stopwatch timeSinceLastMessage = new Stopwatch();
  private final Object lock = new Object(); // Stopwatch is not threadsafe
  private final HBaseCubes<? extends Op> cube;

  InactivePeriodFlusher(HBaseCubes<? extends Op> cube) {
    this.cube = cube;
  }

  /**
   * Consumers should reset the flusher to signal messages are being processed. Marking resets the timer.
   */
  public void reset() {
    synchronized (lock) {
      timeSinceLastMessage.reset().start();
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        synchronized (lock) {
          if (timeSinceLastMessage.isRunning()
            && timeSinceLastMessage.elapsed(TimeUnit.MILLISECONDS) > INACTIVITY_FLUSH_INTERVAL_MSECS) {
            LOG.info("Forcing a cube flush due to period of inactivity [{} msecs]",
              timeSinceLastMessage.elapsed(TimeUnit.MILLISECONDS));
            cube.flush();
            timeSinceLastMessage.reset().start();
          }
        }
        Thread.sleep(SLEEP_TIME_MSECS);
      }
    } catch (InterruptedException e) {
      // most likely we're shutting down (there is a flush on shutdown elsewhere)
    }
  }
}
