package com.xgen.mongot.monitor;

import com.xgen.mongot.util.concurrent.LockGuard;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ToggleGate allows the gate to be explicitly opened or closed through direct method calls. It is
 * useful if a gate is needed in a static state.
 */
public class ToggleGate implements Gate {
  private boolean isOpen;
  private final ReentrantLock lock;
  private final Condition openCondition;
  private final Condition closeCondition;

  private final Clock clock;
  private Instant lastChanged;

  private ToggleGate(boolean isOpen) {
    this.isOpen = isOpen;
    this.lock = new ReentrantLock();
    this.openCondition = this.lock.newCondition();
    this.closeCondition = this.lock.newCondition();
    this.clock = Clock.systemUTC();
    this.lastChanged = this.clock.instant();
  }

  public static ToggleGate opened() {
    return new ToggleGate(true);
  }

  public static ToggleGate closed() {
    return new ToggleGate(false);
  }

  public void open() {
    try (var ignored = LockGuard.with(this.lock)) {
      if (!this.isOpen) {
        this.lastChanged = this.clock.instant();
        this.isOpen = true;
      }
      this.openCondition.signalAll();
    }
  }

  public void close() {
    try (var ignored = LockGuard.with(this.lock)) {
      if (this.isOpen) {
        this.lastChanged = this.clock.instant();
        this.isOpen = false;
      }
      this.closeCondition.signalAll();
    }
  }

  @Override
  public boolean isOpen() {
    try (var ignored = LockGuard.with(this.lock)) {
      return this.isOpen;
    }
  }

  @Override
  public void awaitOpen() throws InterruptedException {
    try (var ignored = LockGuard.with(this.lock)) {
      while (!this.isOpen) {
        this.openCondition.await();
      }
    }
  }

  @Override
  public void awaitClose() throws InterruptedException {
    try (var ignored = LockGuard.with(this.lock)) {
      while (this.isOpen) {
        this.closeCondition.await();
      }
    }
  }

  @Override
  public void update(double usage) {
    // intentionally empty
  }

  @Override
  public Instant lastChanged() {
    return this.lastChanged;
  }

  public String toString() {
    return String.format("ToggleGate{isOpen=%b}", this.isOpen);
  }
}
