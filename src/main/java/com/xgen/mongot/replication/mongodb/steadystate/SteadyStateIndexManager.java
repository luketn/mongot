package com.xgen.mongot.replication.mongodb.steadystate;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.util.Crash;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

class SteadyStateIndexManager {

  private final Supplier<ChangeStreamResumeInfo> resumeInfoSupplier;
  private final CompletableFuture<Void> future;

  private SteadyStateIndexManager(
      Supplier<ChangeStreamResumeInfo> resumeInfoSupplier, CompletableFuture<Void> future) {
    this.resumeInfoSupplier = resumeInfoSupplier;
    this.future = future;
  }

  static SteadyStateIndexManager create(
      Supplier<ChangeStreamResumeInfo> resumeInfoSupplier,
      CompletableFuture<Void> changeStreamLifecycleFuture) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    // When the supplied future fails, fail the SteadyStateIndexManager's future with the
    // same exception.
    Crash.because("failed consuming change stream")
        .ifCompletesExceptionally(
            changeStreamLifecycleFuture.handle(
                (ignored, throwable) -> {
                  checkState(
                      throwable != null,
                      "changeStreamLifecycleFuture completed without an exception");

                  Throwable unwrapped =
                      throwable instanceof CompletionException ? throwable.getCause() : throwable;
                  future.completeExceptionally(unwrapped);
                  return null;
                }));

    return new SteadyStateIndexManager(resumeInfoSupplier, future);
  }

  ChangeStreamResumeInfo getResumeInfo() {
    return this.resumeInfoSupplier.get();
  }

  CompletableFuture<Void> getFuture() {
    return this.future;
  }
}
