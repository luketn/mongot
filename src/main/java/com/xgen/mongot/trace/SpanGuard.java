package com.xgen.mongot.trace;

import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
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

  @MustBeClosed
  SpanGuard(Span span, Scope scope) {
    this.span = span;
    this.scope = scope;
  }

  /**
   * MustBeClosedChecker is suppressed, as it cannot detect that the scope is implicitly closed when
   * a SpanGuard closes.
   */
  @MustBeClosed
  @SuppressWarnings("MustBeClosedChecker")
  static SpanGuard fromSpan(Span span) {
    @Var Scope scope;
    try {
      scope = span.makeCurrent();
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("spanId", span.getSpanContext().getSpanId())
          .log("Span unable to be made current. Scope set to noop.");
      scope = Scope.noop();
    }
    return new SpanGuard(span, scope);
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
      this.span.end();
    }
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
