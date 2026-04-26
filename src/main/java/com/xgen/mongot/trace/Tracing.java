package com.xgen.mongot.trace;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.Var;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanLimits;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the OpenTelemetry Tracer. Helper methods are provided to simplify implementation of
 * tracing for methods of interest. Many methods return guards, or wrapper classes for spans, which
 * undertake the responsibility of closing / ending spans for you.
 */
public class Tracing {
  private static final Logger LOG = LoggerFactory.getLogger(Tracing.class);

  /**
   * The default OpenTelemetry tracer for the mongot application, which uses the global default SDK
   * configuration initialized in {@link Tracing}. All helper methods in {@link Tracing} use {@code
   * TRACER} by default.
   */
  public static final Tracer TRACER;

  /**
   * When using {@link ToggleSampler}, an {@link AttributeKey} to toggle sampling on or off.:
   *
   * <pre>{@code
   * Attributes atr = Attributes.of(TOGGLE_TRACE, true);
   *
   * }</pre>
   *
   * <p>While {@code TOGGLE_TRACE} is more verbose than using {@link Tracing#TOGGLE_ON} or {@link
   * Tracing#TOGGLE_OFF}, it allows for adding other attributes at initialization:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo",
   *    Attributes.of(TOGGLE_TRACE, true, AttributeKey.doubleKey("myNum"), 2.0));
   *
   * }</pre>
   */
  public static final AttributeKey<Boolean> TOGGLE_TRACE = ToggleSampler.TOGGLE_TRACE;

  /**
   * When using {@link ToggleSampler}, an {@link Attributes} object to toggle sampling on:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo", TOGGLE_ON);
   * }</pre>
   *
   * <p>Note that while {@code TOGGLE_ON} is more succinct than using {@link Tracing#TOGGLE_TRACE},
   * it does not allow you to add any additional attributes to a new span at initialization.
   */
  public static final Attributes TOGGLE_ON = Attributes.of(ToggleSampler.TOGGLE_TRACE, true);

  /**
   * When using {@link ToggleSampler}, an {@link Attributes} object to toggle sampling off:
   *
   * <pre>{@code
   * Span sp = Tracing.unguardedSpan("foo", TOGGLE_OFF);
   *
   * }</pre>
   *
   * <p>Note that while {@code TOGGLE_OFF} is more succinct than using {@link Tracing#TOGGLE_TRACE},
   * it does not allow you to add any additional attributes to a new span at initialization.
   */
  public static final Attributes TOGGLE_OFF = Attributes.of(ToggleSampler.TOGGLE_TRACE, false);

  /**
   * An OpenTelemetry random {@link IdGenerator}. Use {@link IdGenerator#generateTraceId()} and
   * {@link IdGenerator#generateSpanId()} to create random TraceIds and SpanIds.
   */
  @VisibleForTesting static final IdGenerator ID_GENERATOR = IdGenerator.random();

  private static final SdkTracerProvider TRACER_PROVIDER;
  private static final String DEFAULT_REMOTE_SPAN_ID = ID_GENERATOR.generateSpanId();
  private static final ToggleSampler sampler = new ToggleSampler();
  private static final boolean DETAILED_TRACE_SPANS =
      Boolean.parseBoolean(
          System.getenv().getOrDefault("DETAILED_TRACE_SPANS", "false").toLowerCase(Locale.ROOT));
  private static final boolean DETAILED_TRACE_PAYLOADS =
      Boolean.parseBoolean(
          System.getenv()
              .getOrDefault("DETAILED_TRACE_PAYLOADS", Boolean.toString(DETAILED_TRACE_SPANS))
              .toLowerCase(Locale.ROOT));
  private static final double TRACE_PAYLOAD_SAMPLE_RATE =
      parseDoubleEnv("TRACE_PAYLOAD_SAMPLE_RATE", 1.0, 0.0, 1.0);
  private static final int TRACE_PAYLOAD_MAX_CHARS =
      parseIntEnv("TRACE_PAYLOAD_MAX_CHARS", 4096, 1, 1_000_000);
  private static final int TRACE_PAYLOAD_SAMPLE_DOCS =
      parseIntEnv("TRACE_PAYLOAD_SAMPLE_DOCS", 2, 0, 100);

  static {
    Resource resource =
        Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "mongot")));

    TRACER_PROVIDER =
        SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(createSpanExporter()).build())
            .setSpanLimits(
                SpanLimits.getDefault()
                    .toBuilder()
                    .setMaxAttributeValueLength(Math.max(1000, TRACE_PAYLOAD_MAX_CHARS))
                    .build())
            .setSampler(Sampler.parentBased(sampler))
            .setResource(resource)
            .build();

    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(TRACER_PROVIDER).build();

    TRACER = openTelemetry.getTracer("mongot-tracing", "0.0.1");
  }

  private static SpanExporter createSpanExporter() {
    String tracesExporter =
        System.getProperty(
            "otel.traces.exporter", System.getenv().getOrDefault("OTEL_TRACES_EXPORTER", ""));
    if (!"otlp".equalsIgnoreCase(tracesExporter)) {
      return Slf4jExporter.create();
    }

    return OtlpHttpSpanExporter.builder().setEndpoint(getOtlpHttpTraceEndpoint()).build();
  }

  private static String getOtlpHttpTraceEndpoint() {
    @Var
    String endpoint =
        System.getProperty(
            "otel.exporter.otlp.traces.endpoint",
            System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", ""));
    if (endpoint.isBlank()) {
      endpoint =
          System.getProperty(
              "otel.exporter.otlp.endpoint",
              System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"));
    }
    if (endpoint.endsWith("/v1/traces")) {
      return endpoint;
    }
    return endpoint.replaceAll("/+$", "") + "/v1/traces";
  }

  /**
   * Sets mongot's global ToggleSampler instance to have a new default sampling-rate. Should only be
   * used by {@code MmsMongotBootstrapper.java} to update the default sampling-rate, when mongot
   * receives an MmsConfig file at startup.
   *
   * <p>Note: assumes the use of {@link ToggleSampler} as mongot's global {@link Sampler} instance
   * for tracing.
   *
   * @param newRate the new default sampling-rate. Must be in the range [0,1]. If {@code newRate} is
   *     not in this range, the sampling-rate will not be updated.
   */
  public static void updateSamplingRate(double newRate) {
    double oldRate = sampler.getDefaultRatio();
    if (newRate < 0.0 || newRate > 1.0) { // not a valid rate
      LOG.atInfo()
          .addKeyValue("oldRate", oldRate)
          .addKeyValue("newRate", newRate)
          .log(
              "Tried to update OpenTelemetry tracing sampling-rate, but the new rate was invalid.");
    } else {
      sampler.updateDefaultSampleRate(newRate);
      LOG.atInfo()
          .addKeyValue("oldRate", oldRate)
          .addKeyValue("newRate", newRate)
          .log("OpenTelemetry tracing sampling-rate updated.");
    }
  }

  /**
   * Note: assumes the use of {@link ToggleSampler} as mongot's global {@link Sampler} instance for
   * tracing.
   *
   * @return the default sampling-rate of the tracer
   */
  public static double getDefaultSamplingRate() {
    return sampler.getDefaultRatio();
  }

  /**
   * Creates a {@link SpanGuard}, that wraps around a newly created {@link Span} and opens a new
   * {@link Scope} around the span. When called within a try-with-resources block, the SpanGuard
   * will auto-close the span and its associated scope.
   *
   * @param name The name of the span
   * @return The created SpanGuard
   */
  @MustBeClosed
  public static SpanGuard simpleSpanGuard(String name) {
    return simpleSpanGuard(name, Attributes.empty());
  }

  /**
   * Creates a {@link SpanGuard}, that wraps around a newly created {@link Span} and opens a new
   * {@link Scope} around the span. When called within a try-with-resources block, the SpanGuard
   * will auto-close the span and its associated scope.
   *
   * @param name the name of the span
   * @param attributes any initial {@link Attributes} of the span
   * @return the created SpanGuard
   */
  @MustBeClosed
  public static SpanGuard simpleSpanGuard(String name, Attributes attributes) {
    Span span = unguardedSpan(name, attributes);
    return SpanGuard.fromSpan(span, detailedSpanMetricName(name));
  }

  /**
   * Creates a span only when {@code DETAILED_TRACE_SPANS=true}. Use this for expensive diagnostic
   * spans that should not even be allocated on the default production path.
   */
  @MustBeClosed
  public static SpanGuard detailedSpanGuard(String name) {
    return detailedSpanGuard(name, Attributes.empty());
  }

  /**
   * Creates a span only when {@code DETAILED_TRACE_SPANS=true}. Use this for expensive diagnostic
   * spans that should not even be allocated on the default production path.
   */
  @MustBeClosed
  public static SpanGuard detailedSpanGuard(String name, Attributes attributes) {
    if (!isDetailedTraceSpansEnabled()) {
      return SpanGuard.noop();
    }
    return simpleSpanGuard(name, attributes);
  }

  /**
   * Creates a span only when {@code DETAILED_TRACE_SPANS=true}, without making the span current.
   * Use this for lifecycle spans that need to cross callbacks or threads, where keeping a
   * thread-local {@link Scope} open would be incorrect.
   */
  public static Optional<Span> detailedUnguardedSpan(String name, Attributes attributes) {
    if (!isDetailedTraceSpansEnabled()) {
      return Optional.empty();
    }
    return Optional.of(unguardedSpan(name, attributes));
  }

  /**
   * Attributes for a search command root span. When detailed tracing is enabled the root span is
   * force-sampled and all descendant spans inherit that decision. When disabled, the current
   * ToggleSampler keeps the root span dropped.
   */
  public static Attributes rootSpanAttributes(Attributes attributes) {
    return Attributes.builder()
        .putAll(attributes)
        .put(TOGGLE_TRACE, isDetailedTraceSpansEnabled())
        .build();
  }

  public static boolean isDetailedTraceSpansEnabled() {
    return DETAILED_TRACE_SPANS;
  }

  public static boolean shouldCaptureTracePayloads(Span span) {
    if (!isDetailedTraceSpansEnabled()
        || !DETAILED_TRACE_PAYLOADS
        || TRACE_PAYLOAD_SAMPLE_RATE <= 0.0
        || !span.getSpanContext().isValid()) {
      return false;
    }
    if (TRACE_PAYLOAD_SAMPLE_RATE >= 1.0) {
      return true;
    }

    long positiveHash = Integer.toUnsignedLong(span.getSpanContext().getTraceId().hashCode());
    double normalized = positiveHash / (double) (1L << 32);
    return normalized < TRACE_PAYLOAD_SAMPLE_RATE;
  }

  public static int tracePayloadSampleDocs() {
    return TRACE_PAYLOAD_SAMPLE_DOCS;
  }

  public static void setPayloadAttribute(Span span, String attributeName, String value) {
    if (!shouldCaptureTracePayloads(span) || value == null) {
      return;
    }

    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    boolean truncated = value.length() > TRACE_PAYLOAD_MAX_CHARS;
    String emitted = truncated ? value.substring(0, TRACE_PAYLOAD_MAX_CHARS) : value;

    span.setAttribute(attributeName, emitted);
    span.setAttribute(attributeName + ".sha256", sha256Hex(utf8));
    span.setAttribute(attributeName + ".size_bytes", utf8.length);
    span.setAttribute(attributeName + ".truncated", truncated);
  }

  private static String sha256Hex(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the JDK", e);
    }
  }

  private static Optional<String> detailedSpanMetricName(String name) {
    if (!isDetailedTraceSpansEnabled()) {
      return Optional.empty();
    }
    return name.startsWith("mongot.search.")
            || name.startsWith("mongot.vector_search.")
            || name.startsWith("mongot.cursor.")
            || name.startsWith("mongot.lucene.")
            || name.startsWith("mongot.grpc.")
            || name.startsWith("mongot.executor.")
        ? Optional.of(name)
        : Optional.empty();
  }

  /**
   * Creates a new span with the given {@code name}, without any initial attributes. The method
   * starts the {@link Span}, but is not responsible with ending the span, or opening its scope.
   *
   * @param name the name of the span
   * @return the newly created Span
   */
  public static Span unguardedSpan(String name) {
    return unguardedSpan(name, Attributes.empty());
  }

  /**
   * Creates a new span with the given {@code name} and initialized with the given {@code
   * attributes}. The method starts the {@link Span}, but is not responsible with ending the span,
   * or opening its scope.
   *
   * @param name the name of the span
   * @param attributes any initial {@link Attributes} of the span
   * @return the newly created Span
   */
  public static Span unguardedSpan(String name, Attributes attributes) {
    return TRACER.spanBuilder(name).setAllAttributes(attributes).startSpan();
  }

  /**
   * Creates a new child {@link Span} which has its parentSpanId pointing to {@code parent}. Is
   * responsible for returning opening and closing the child span and {@link Scope}, and
   * activating/closing the parent scope. It is NOT responsible for ending the parent span, nor
   * creating the parent span.
   *
   * <p>Note that since the method opens the parent's scope, any other spans created during the
   * lifespan of the returned {@link ParentScopeGuard} will be placed as a child underneath the
   * {@code parent}.
   *
   * @param parent the parent Span of the child Span
   * @param childName the name of the new child span
   * @return a {@link ParentScopeGuard}, which wraps the parent scope and the child {@link
   *     SpanGuard}
   */
  @MustBeClosed
  public static ParentScopeGuard childUnderActivatedParent(Span parent, String childName) {
    return childUnderActivatedParent(parent, childName, Attributes.empty());
  }

  /**
   * Creates a new child {@link Span} which has its parentSpanId pointing to {@code parent}. Is
   * responsible for returning opening and closing the child span and {@link Scope}, and
   * activating/closing the parent scope. It is NOT responsible for ending the parent span, nor
   * creating the parent span.
   *
   * <p>Note that since the method opens the parent's scope, any other spans created during the
   * lifespan of the returned {@link ParentScopeGuard} will be placed as a child underneath the
   * {@code parent}.
   *
   * @param parent the parent Span of the child Span
   * @param childName the name of the new child span
   * @param childAttributes initial {@link Attributes} for the child span
   * @return a {@link ParentScopeGuard}, which wraps the parent scope and the child {@link
   *     SpanGuard}
   */
  @MustBeClosed
  public static ParentScopeGuard childUnderActivatedParent(
      Span parent, String childName, Attributes childAttributes) {
    return ParentScopeGuard.withChild(parent, childName, childAttributes);
  }

  /**
   * Creates a {@link Span} underneath a parent context with custom {@link TraceId} of {@code
   * traceId} and custom {@link TraceFlags} of {@code traceFlags}. This means:
   *
   * <ul>
   *   <li>the created span has the same traceId as its parent context.
   *   <li>the created span has the same sampling decision as its parent context. This is because by
   *       default, the global sampler will propagate a sampling decision of a context to all
   *       children created underneath that context.
   *       <ul>
   *         <li>The parent context's sampling decision is held within and determined by the
   *             parameter {@code traceFlags} (i.e. one of the bits of {@code traceFlags} is a
   *             sampling bit).
   *       </ul>
   * </ul>
   *
   * <p>Note that the {@code traceId} and {@code traceFlags} are associated with a parent context,
   * but the parent context is not linked to a real parent span:
   *
   * <ul>
   *   <li>there's no parent span that starts or ends.
   *   <li>there's no parent span that has an active scope open, and you never have to worry about
   *       closing a scope.
   *   <li>there's no parent span that is sampled.
   *   <li>there's no parent span actively kept in memory.
   * </ul>
   *
   * <p>The parent context is to create a common ancestor for multiple sibling spans. Note how this
   * method differs from {@link Tracing#childUnderActivatedParent(Span, String)}, as here, the
   * "parent" span is not real.
   *
   * <p>Note: the new span does not accept initial attributes, i.e. it should not be manually
   * sampled by a {@link Tracing#TOGGLE_ON} attribute. This is to ensure that children created under
   * the parent context have the same sampling decision, i.e. the parent context's sampling
   * decision.
   *
   * <p>Sample usage:
   *
   * <pre>{@code
   * String traceId;
   * TraceFlags cachedFlags;
   * try (SpanGuard sibling1 = Tracing.simpleSpanGuard("sibling1")) {
   *   traceId = sibling1.getSpan().getSpanContext().getTraceId();
   *   cachedFlags = sibling1.getSpan().getSpanContext().getTraceFlags();
   * }
   * try (SpanGuard sibling2 = Tracing.withTraceId("sibling2", traceId, cachedFlags) {
   *   // do something
   * }
   * }</pre>
   *
   * @param name the name of the created child span
   * @param traceId the traceId of the parent context under which you place the child span. The
   *     child span will have the same traceId.
   * @param traceFlags the traceFlags of the parent context under which you place the child span.
   *     The child span will have the same traceFlags (includes sampling decision).
   * @return the created SpanGuard wrapping the child span
   */
  @MustBeClosed
  public static SpanGuard withTraceId(String name, String traceId, TraceFlags traceFlags) {
    SpanContext parentContext =
        SpanContext.createFromRemoteParent(
            traceId, DEFAULT_REMOTE_SPAN_ID, traceFlags, TraceState.getDefault());

    Span span =
        TRACER
            .spanBuilder(name)
            .setAllAttributes(Attributes.empty())
            .setParent(Context.current().with(Span.wrap(parentContext)))
            .startSpan();

    return SpanGuard.fromSpan(span);
  }

  /**
   * If {@code TRACER} uses a {@link BatchSpanProcessor}, this method force flushes any spans
   * waiting in a batch, ready to be exported. This is <b>not</b> recommended to be used, other than
   * for diagnostic/testing purposes.
   *
   * @return the {@link CompletableResultCode} which OpenTelemetry tracers provide when they are
   *     force flushed
   */
  @VisibleForTesting
  static CompletableResultCode forceFlush() {
    return TRACER_PROVIDER.forceFlush();
  }

  private static double parseDoubleEnv(String name, double defaultValue, double min, double max) {
    String value = System.getenv().get(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      double parsed = Double.parseDouble(value);
      if (parsed < min || parsed > max) {
        LOG.atWarn()
            .addKeyValue("name", name)
            .addKeyValue("value", value)
            .addKeyValue("defaultValue", defaultValue)
            .log("Ignoring out-of-range tracing environment value.");
        return defaultValue;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOG.atWarn()
          .addKeyValue("name", name)
          .addKeyValue("value", value)
          .addKeyValue("defaultValue", defaultValue)
          .log("Ignoring invalid tracing environment value.");
      return defaultValue;
    }
  }

  private static int parseIntEnv(String name, int defaultValue, int min, int max) {
    String value = System.getenv().get(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < min || parsed > max) {
        LOG.atWarn()
            .addKeyValue("name", name)
            .addKeyValue("value", value)
            .addKeyValue("defaultValue", defaultValue)
            .log("Ignoring out-of-range tracing environment value.");
        return defaultValue;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOG.atWarn()
          .addKeyValue("name", name)
          .addKeyValue("value", value)
          .addKeyValue("defaultValue", defaultValue)
          .log("Ignoring invalid tracing environment value.");
      return defaultValue;
    }
  }
}
