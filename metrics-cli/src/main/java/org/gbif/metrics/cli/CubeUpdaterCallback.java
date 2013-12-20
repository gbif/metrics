package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.metrics.cube.HBaseCubes;

import java.util.concurrent.TimeUnit;

import com.sun.org.apache.bcel.internal.generic.NEW;
import com.urbanairship.datacube.AsyncException;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.Op;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class to simplify writing message callbacks that mutate cubes by providing the message handling and the timing
 * services.
 *
 * @param <T> The cube type
 */
abstract class CubeUpdaterCallback<T extends Op> extends AbstractMessageCallback<OccurrenceMutatedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(CubeUpdaterCallback.class);
  private final Counter messageCount = Metrics.newCounter(getClass(), "messageCount");
  private final Counter newOccurrencesCount = Metrics.newCounter(getClass(), "newOccurrencesCount");
  private final Counter updatedOccurrencesCount = Metrics.newCounter(getClass(), "updatedOccurrencesCount");
  private final Counter deletedOccurrencesCount = Metrics.newCounter(getClass(), "deletedOccurrencesCount");
  private final Timer writeTimer = Metrics.newTimer(getClass(), "cubeWrites", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private InactivePeriodFlusher inactivePeriodFlusher;

  abstract HBaseCubes<T> getCube();

  protected abstract Batch<T> getNewMutations(Occurrence occurrence);

  protected abstract Batch<T> getUpdateMutations(Occurrence oldOccurrence, Occurrence newOccurrence);

  @Override
  public void handleMessage(OccurrenceMutatedMessage message) {
    LOG.debug("Handling {} occurrence", message.getStatus());
    messageCount.inc();
    resetInactiveWatcher();

    Batch<T> update = new Batch<T>();
    switch (message.getStatus()) {
      case NEW:
        newOccurrencesCount.inc();
        update = getNewMutations(message.getNewOccurrence());
        break;
      case UPDATED:
        updatedOccurrencesCount.inc();
        // if there's no interesting difference btw occurrences, ignore this msg
        if (OccurrenceComparisonUtil.equivalent(message.getOldOccurrence(), message.getNewOccurrence())) {
          LOG.debug("Ignoring update on occurrence [{}] because cube related fields haven't changed.",
            message.getOldOccurrence().getKey());
          return;
        }
        update = getUpdateMutations(message.getOldOccurrence(), message.getNewOccurrence());
        break;
      case DELETED:
        deletedOccurrencesCount.inc();
        update = getUpdateMutations(message.getOldOccurrence(), null);
        break;
      case UNCHANGED:
        return;
    }

    TimerContext context = writeTimer.time();
    try {
      if (update.getMap() != null && !update.getMap().isEmpty()) {
        LOG
          .debug("Mutations for {} occurrence produced {} cube mutations", message.getStatus(), update.getMap().size());
        getCube().write(update);
      } else {
        LOG.debug("No mutations for {} occurrence produced. Old[{}] New[{}], ", message.getStatus(),
          message.getOldOccurrence(), message.getNewOccurrence());
      }
    } catch (AsyncException e) {
      LOG.error("Exception while updating cube for [{}] Occurrence [{}]", message.getStatus(), e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while updating cube for [{}] Occurrence [{}]", message.getStatus(), e);
    } finally {
      context.stop();
    }
    LOG.debug("Finished handling {} occurrence", message.getStatus());
  }

  protected void resetInactiveWatcher() {
    if (inactivePeriodFlusher != null) {
      inactivePeriodFlusher.reset();
    }
  }

  /**
   * Starts an inactive watcher if there is not one already running.
   */
  public synchronized void startInactiveWatcher() {
    if (inactivePeriodFlusher == null) {
      LOG.info("Starting watcher thread to flush cube on periods of inactivity");
      inactivePeriodFlusher = new InactivePeriodFlusher(getCube());
      Thread t = new Thread(inactivePeriodFlusher);
      // TODO: if we were to do the following, a flush might be terminated on shutdown
      // however, will it block shutdown otherwise?
      // t.setDaemon(true); // we're just a background thread
      t.start();
    }
  }
}
