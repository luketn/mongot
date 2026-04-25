package com.xgen.mongot.trace;

import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.GlobalMetricFactory;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class around a {@link Span}, which serves to make a span {@link AutoCloseable}. When
 * constructed, a {@link SpanGuard} will also create a current {@link Scope} associated with span.
 * When an instance of the class closes, it will close both the span and the associated scope.
 */
public class SpanGuard implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SpanGuard.class);
  private final Span span;
  private final Scope scope;
  private final Optional<String> detailedMetricSpanName;
  private final long detailedMetricStartNanos;

  @MustBeClosed
  SpanGuard(Span span, Scope scope) {
    this(span, scope, Optional.empty());
  }

  @MustBeClosed
  SpanGuard(Span span, Scope scope, Optional<String> detailedMetricSpanName) {
    this.span = span;
    this.scope = scope;
    this.detailedMetricSpanName = detailedMetricSpanName;
    this.detailedMetricStartNanos = detailedMetricSpanName.isPresent() ? System.nanoTime() : 0;
  }

  /**
   * MustBeClosedChecker is suppressed, as it cannot detect that the scope is implicitly closed when
   * a SpanGuard closes.
   */
  @MustBeClosed
  @SuppressWarnings("MustBeClosedChecker")
  static SpanGuard fromSpan(Span span) {
    return fromSpan(span, Optional.empty());
  }

  /**
   * MustBeClosedChecker is suppressed, as it cannot detect that the scope is implicitly closed when
   * a SpanGuard closes.
   */
  @MustBeClosed
  @SuppressWarnings("MustBeClosedChecker")
  static SpanGuard fromSpan(Span span, Optional<String> detailedMetricSpanName) {
    @Var Scope scope;
    try {
      scope = span.makeCurrent();
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("spanId", span.getSpanContext().getSpanId())
          .log("Span unable to be made current. Scope set to noop.");
      scope = Scope.noop();
    }
    return new SpanGuard(span, scope, detailedMetricSpanName);
  }

  @MustBeClosed
  static SpanGuard noop() {
    return new SpanGuard(Span.getInvalid(), Scope.noop(), Optional.empty());
  }

  @Override
  public void close() {
    try {
      this.scope.close();
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("spanId", this.span.getSpanContext().getSpanId())
          .log("Spanguard unable to close scope.");
    } finally {
      try {
        this.span.end();
      } finally {
        recordDetailedSpanMetric();
      }
    }
  }

  private void recordDetailedSpanMetric() {
    this.detailedMetricSpanName.ifPresent(
        spanName -> {
          try {
            GlobalMetricFactory.recordDetailedTraceSpan(
                spanName, System.nanoTime() - this.detailedMetricStartNanos);
          } catch (Exception e) {
            LOG.atWarn().addKeyValue("spanName", spanName).log("Unable to record span metric.");
          }
        });
  }

  /**
   * Should not be used for opening/closing the span, or activating/deactivating its scope. The
   * SpanGuard does this on its own. Proper use cases include adding more attributes to the span.
   *
   * @return the {@link Span} object associated with the SpanGuard object.
   */
  public Span getSpan() {
    return this.span;
  }
}
