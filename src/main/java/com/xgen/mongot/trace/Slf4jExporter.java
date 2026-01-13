package com.xgen.mongot.trace;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SegmentedStringWriter;
import io.opentelemetry.exporter.internal.otlp.traces.ResourceSpansMarshaler;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SpanExporter} which writes {@linkplain SpanData spans} to a {@link Logger} in OTLP JSON
 * format. Each log line will include a single {@code ResourceSpans}.
 */
public final class Slf4jExporter implements SpanExporter {

  private static final Logger logger = LoggerFactory.getLogger(Slf4jExporter.class);

  private final AtomicBoolean isShutdown = new AtomicBoolean();

  /**
   * Returns a new {@link Slf4jExporter}, which exports traces in OTLP-JSON format, through the
   * project's Slf4j logger, rather than the standard Java util logger.
   */
  public static SpanExporter create() {
    return new Slf4jExporter();
  }

  private Slf4jExporter() {}

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    if (this.isShutdown.get()) {
      return CompletableResultCode.ofFailure();
    }

    ResourceSpansMarshaler[] allResourceSpans = ResourceSpansMarshaler.create(spans);
    for (ResourceSpansMarshaler resourceSpans : allResourceSpans) {
      SegmentedStringWriter sw =
          new SegmentedStringWriter(JsonUtil.JSON_FACTORY._getBufferRecycler());
      try (JsonGenerator gen = JsonUtil.create(sw)) {
        resourceSpans.writeJsonToGenerator(gen);
      } catch (IOException e) {
        // Shouldn't happen in practice, just skip it.
        continue;
      }
      try {
        logger.info(sw.getAndClear());
      } catch (IOException e) {
        logger.warn("Unable to read OTLP JSON spans", e);
      }
    }
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    if (!this.isShutdown.compareAndSet(false, true)) {
      logger.info("Calling shutdown() multiple times.");
    }
    return CompletableResultCode.ofSuccess();
  }

  private static final class JsonUtil {

    static final JsonFactory JSON_FACTORY = new JsonFactory();

    static JsonGenerator create(SegmentedStringWriter stringWriter) {
      try {
        return JSON_FACTORY.createGenerator(stringWriter);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Unable to create in-memory JsonGenerator, can't happen.", e);
      }
    }

    private JsonUtil() {}
  }
}
