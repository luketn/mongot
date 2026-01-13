package com.xgen.mongot.monitor;

import com.xgen.mongot.util.concurrent.LockGuard;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HysteresisGate opens and closes based on the following rules: - The gate is initialized as open.
 * - When the gate is open and the value exceeds the close threshold, the gate closes. - When the
 * gate is closed and the value drops below the open threshold, then gate opens. Specifying an open
 * threshold lower than the close threshold prevents flapping.
 */
public class HysteresisGate implements Gate {

  private final double openThreshold;
  private final double closeThreshold;
  private double value;

  private final ReentrantLock lock;
  private final Condition openCondition;
  private final Condition closeCondition;
  private boolean isOpen;

  private final Clock clock;
  private Instant lastChanged;

  public HysteresisGate(double openThreshold, double closeThreshold) {
    this.openThreshold = openThreshold;
    this.closeThreshold = closeThreshold;
    this.value = 0;
    this.lock = new ReentrantLock();
    this.openCondition = this.lock.newCondition();
    this.closeCondition = this.lock.newCondition();
    this.isOpen = true;
    this.clock = Clock.systemUTC();
    this.lastChanged = this.clock.instant();
  }

  @Override
  public void update(double currentValue) {
    try (var ignored = LockGuard.with(this.lock)) {
      this.value = currentValue;
      if (this.isOpen && currentValue > this.closeThreshold) {
        this.isOpen = false;
        this.lastChanged = this.clock.instant();
        this.closeCondition.signalAll();
      } else if (!this.isOpen && currentValue < this.openThreshold) {
        this.isOpen = true;
        this.lastChanged = this.clock.instant();
        this.openCondition.signalAll();
      }
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
  public Instant lastChanged() {
    try (var ignored = LockGuard.with(this.lock)) {
      return this.lastChanged;
    }
  }

  public String toString() {
    return String.format(
        "HysteresisGate{isOpen=%b,lastObservedUsage=%.2f,closeThreshold=%.2f,openThreshold=%.2f}",
        this.isOpen, this.value, this.closeThreshold, this.openThreshold);
  }
}
