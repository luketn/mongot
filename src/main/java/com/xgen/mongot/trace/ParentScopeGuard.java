package com.xgen.mongot.trace;

import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class around a parent span's {@link Scope}, and a {@link SpanGuard} that wraps a child
 * Span. Is meant to be used to conveniently open/close a child span, as well as the scope of the
 * parent.
 *
 * <p>Note that on creation, the parent's scope is opened, meaning that any other spans created
 * during the lifespan of the returned {@link ParentScopeGuard} will be placed as a child underneath
 * the {@code parent}.
 *
 * <p>It is NOT responsible for:
 *
 * <ul>
 *   <li>Ending the parent span.
 *   <li>Creating the parent span; it should be pre-initialized when being passed in.
 * </ul>
 *
 * <p>The class assumes that the creation/ending of the parent span is handled elsewhere.
 */
public class ParentScopeGuard implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ParentScopeGuard.class);
  private final SpanGuard childSpanGuard;
  private final Span parentSpan;
  private final Scope parentScope;

  @MustBeClosed
  private ParentScopeGuard(Span parentSpan, Scope parentScope, SpanGuard childSpanGuard) {
    this.childSpanGuard = childSpanGuard;
    this.parentSpan = parentSpan;
    this.parentScope = parentScope;
  }

  /**
   * MustBeClosedChecker is suppressed, as it cannot detect that the scope is implicitly closed when
   * a SpanGuard closes.
   */
  @MustBeClosed
  @SuppressWarnings("MustBeClosedChecker")
  static ParentScopeGuard withChild(Span parentSpan, String childName, Attributes childAttributes) {
    @Var Scope parentScope;
    try {
      parentScope = parentSpan.makeCurrent();
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("spanId", parentSpan.getSpanContext().getSpanId())
          .log("parentSpan unable to be made current. Scope set to noop.");
      parentScope = Scope.noop();
    }

    @Var SpanGuard childSpanGuard;
    try {
      childSpanGuard = Tracing.simpleSpanGuard(childName, childAttributes);
    } catch (Exception e) {
      LOG.warn("child spanGuard unable to be created. Child set to non-recording span.");
      childSpanGuard = SpanGuard.fromSpan(Span.getInvalid());
    }
    return new ParentScopeGuard(parentSpan, parentScope, childSpanGuard);
  }

  @Override
  public void close() {
    try {
      this.childSpanGuard.close();
    } catch (Exception e1) {
      LOG.atWarn()
          .addKeyValue("spanId", this.childSpanGuard.getSpan().getSpanContext().getSpanId())
          .log("Spanguard unable to close scope.");
    } finally {
      this.parentScope.close();
    }
  }

  /**
   * Should not be used for opening/closing the span, or activating/deactivating its scope. the
   * guard does this on its own. Proper use cases include adding more attributes to the span.
   *
   * @return the parent {@link Span} associated with the guard object.
   */
  public Span getParentSpan() {
    return this.parentSpan;
  }

  /**
   * Should not be used to open/close the child {@link SpanGuard}. Proper use cases include adding
   * more attributes to the span.
   *
   * @return the SpanGuard the wraps the child span, associated with the ParentScopeGuard.
   */
  public SpanGuard getChildSpanGuard() {
    return this.childSpanGuard;
  }
}
