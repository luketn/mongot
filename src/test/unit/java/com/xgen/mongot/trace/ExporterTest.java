package com.xgen.mongot.trace;

import static org.junit.Assert.assertEquals;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.xgen.testing.TestUtils;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ExporterTest {
  private static final Logger slf4jExporterLogger = TestUtils.getClassLogger(Slf4jExporter.class);

  @Test
  public void testBatchExport() {
    Tracing.forceFlush().join(5L, TimeUnit.SECONDS);
    List<ILoggingEvent> list = TestUtils.getLogEvents(slf4jExporterLogger);

    try (SpanGuard s1 =
        Tracing.simpleSpanGuard(
            "span1", Attributes.of(AttributeKey.booleanKey("toggleTrace"), true))) {
      try (SpanGuard s2 =
          Tracing.simpleSpanGuard(
              "span2", Attributes.of(AttributeKey.booleanKey("toggleTrace"), true))) {
        dummyFunction();
      }
    }
    Tracing.forceFlush().join(5L, TimeUnit.SECONDS);

    assertEquals(1, list.size()); // just one batch
  }

  private static int dummyFunction() {
    return 1 + 1;
  }
}
