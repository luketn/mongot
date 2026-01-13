package com.xgen.mongot.util.concurrent;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DefaultNamedExecutorService implements NamedExecutorService {

  private final ExecutorService delegate;
  private final String name;
  private final MeterRegistry meterRegistry;

  DefaultNamedExecutorService(ExecutorService delegate, String name, MeterRegistry meterRegistry) {
    this.delegate = delegate;
    this.name = name;
    this.meterRegistry = meterRegistry;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    return this.meterRegistry;
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
    this.removeMetrics();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return this.delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return this.delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return this.delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return this.delegate.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return this.delegate.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return this.delegate.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return this.delegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return this.delegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return this.delegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return this.delegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    this.delegate.execute(command);
  }
}
