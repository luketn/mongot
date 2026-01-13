package com.xgen.testing.fakes.mongodb;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

/**
 * MockMongoChangeStreamCursor attempts to replicate the behavior of a MongoChangeStreamCursor,
 * yielding events as specified by the passed in MockEvents after the duration specified duration
 * has elapsed.
 */
public class MockMongoChangeStreamCursor
    implements MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> {

  /**
   * MockEvent contains information about the behavior the cursor should exhibit the next time
   * next() or tryNext() is called.
   *
   * <p>Either a changestream event document or MongoException can be passed in, and the cursor will
   * either return the document or throw the exception after the specified Duration has passed since
   * the last event was processed.
   */
  public static class MockEvent {
    private final Optional<ChangeStreamDocument<BsonDocument>> event;
    private final Optional<MongoException> exception;
    private final Duration pause;

    public MockEvent(ChangeStreamDocument<BsonDocument> event, Duration pause) {
      this(Optional.of(event), Optional.empty(), pause);
    }

    public MockEvent(MongoException exception, Duration pause) {
      this(Optional.empty(), Optional.of(exception), pause);
    }

    private MockEvent(
        Optional<ChangeStreamDocument<BsonDocument>> event,
        Optional<MongoException> exception,
        Duration pause) {
      this.event = event;
      this.exception = exception;
      this.pause = pause;
    }

    public Optional<ChangeStreamDocument<BsonDocument>> getEvent() {
      return this.event;
    }

    public Optional<MongoException> getException() {
      return this.exception;
    }

    public Duration getPause() {
      return this.pause;
    }
  }

  public static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument("postBatchResumeToken", new BsonBoolean(true));

  private final List<MockEvent> events;
  private int nextEventIndex;
  private Optional<ZonedDateTime> lastEventTime;
  private Optional<BsonDocument> resumeToken;

  private final Lock lock;
  private final Condition closedCondition;
  private volatile boolean closed;

  /** Creates a new mock MongoChangeStreamCursor for the supplied events. */
  public MockMongoChangeStreamCursor(List<MockEvent> events) {
    this.events = events;
    this.nextEventIndex = 0;
    this.lastEventTime = Optional.empty();
    this.resumeToken = Optional.empty();

    this.lock = new ReentrantLock();
    this.closedCondition = this.lock.newCondition();
    this.closed = false;
  }

  @Override
  public BsonDocument getResumeToken() {
    return this.resumeToken.orElse(null);
  }

  @Override
  public void close() {
    this.closed = true;

    this.lock.lock();
    try {
      this.closedCondition.signalAll();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public ChangeStreamDocument<BsonDocument> next() {
    try {
      // If we're out of events, simulate the driver blocking until the cursor is closed.
      if (allEventsProcessed()) {
        awaitCloseIndefinitely();
      }

      // Otherwise block until our pause duration has passed or the cursor is closed.
      while (!this.closed) {
        long millisRemaining = millisUntilNextEvent();

        // If we've blocked long enough, update the last event and return the event.
        if (millisRemaining <= 0) {
          return yieldEvent();
        }

        // Sleep until millisRemaining has passed or the cursor is closed.
        this.lock.lock();
        try {
          this.closedCondition.await(millisRemaining, TimeUnit.MILLISECONDS);
        } finally {
          this.lock.unlock();
        }
      }

      // Can only get here if the cursor was closed.
      throwClosedCursorException();
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }

    throw new AssertionError("unreachable");
  }

  @Override
  public int available() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChangeStreamDocument<BsonDocument> tryNext() {
    // If there are no more events, simply set the postBatchResumeToken and return null to simulate
    // returning empty batches forever.
    if (allEventsProcessed()) {
      setResumeToken(POST_BATCH_RESUME_TOKEN);
      return null;
    }

    // If we've waited long enough, yield the event.
    long millisRemaining = millisUntilNextEvent();
    if (millisRemaining <= 0) {
      return yieldEvent();
    }

    // Otherwise set the postBatchResumeToken to simulate an empty batch and return null.
    setResumeToken(POST_BATCH_RESUME_TOKEN);
    return null;
  }

  @Override
  public ServerCursor getServerCursor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServerAddress getServerAddress() {
    throw new UnsupportedOperationException();
  }

  private long millisUntilNextEvent() {
    if (this.lastEventTime.isEmpty()) {
      this.lastEventTime = Optional.of(ZonedDateTime.now());
    }

    MockEvent nextEvent = getNextEvent();
    ZonedDateTime pauseUntil = this.lastEventTime.get().plus(nextEvent.getPause());
    return ChronoUnit.MILLIS.between(ZonedDateTime.now(), pauseUntil);
  }

  private boolean allEventsProcessed() {
    return this.nextEventIndex >= this.events.size();
  }

  private MockEvent getNextEvent() {
    return this.events.get(this.nextEventIndex);
  }

  private MockEvent consumeNextEvent() {
    MockEvent event = getNextEvent();
    this.nextEventIndex++;
    return event;
  }

  private ChangeStreamDocument<BsonDocument> yieldEvent() {
    this.lastEventTime = Optional.of(ZonedDateTime.now());

    MockEvent nextEvent = consumeNextEvent();
    if (nextEvent.getException().isPresent()) {
      throw nextEvent.getException().get();
    }

    setResumeToken(nextEvent.event.get().getResumeToken());
    return nextEvent.getEvent().get();
  }

  private void setResumeToken(BsonDocument resumeToken) {
    this.resumeToken = Optional.of(resumeToken);
  }

  private void awaitCloseIndefinitely() throws InterruptedException {
    while (!this.closed) {
      this.lock.lock();
      try {
        this.closedCondition.await();
      } finally {
        this.lock.unlock();
      }
    }
    throwClosedCursorException();
  }

  private void throwClosedCursorException() {
    throw new MongoException("state should be: open");
  }
}
