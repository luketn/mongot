package com.xgen.mongot.util.concurrent;

import static com.xgen.mongot.util.Check.checkState;

import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * OneShotSingleThreadExecutor is an Executor that has a single Thread and can execute a single task
 * on that thread.
 *
 * <p>This can be useful in cases where you want to use the Executor ecosystem (such as creating a
 * CompletableFuture), but also need access to the Thread that it is being run on (e.g. to interrupt
 * it later).
 */
public class OneShotSingleThreadExecutor implements Executor {

  private final String name;

  private boolean started;
  private Optional<Thread> thread;

  /** Constructs a new OneShotSingleThreadExecutor, creating a Thread with the given name. */
  public OneShotSingleThreadExecutor(String name) {
    this.name = name;
    this.started = false;
    this.thread = Optional.empty();
  }

  @Override
  public synchronized void execute(Runnable runnable) {
    checkState(!this.started, "OneShotSingleThreadExecutor has already been started");

    Thread thread = new Thread(runnable, this.name);
    thread.start();

    this.started = true;
    this.thread = Optional.of(thread);
  }

  /**
   * Returns the Thread used to execute the task.
   *
   * <p>Throws an IllegalStateException if a task hasn't been executed yet.
   */
  public synchronized Thread getThread() {
    checkState(this.started, "OneShotSingleThreadExecutor has not been started yet");
    checkState(
        this.thread.isPresent(),
        "OneShotSingleThreadExecutor has been started but Thread is not present");
    return this.thread.get();
  }
}
