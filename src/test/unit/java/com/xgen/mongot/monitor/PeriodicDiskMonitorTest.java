package com.xgen.mongot.monitor;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.Condition;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.FileStore;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PeriodicDiskMonitorTest {

  private FileStore fileStore;
  private PeriodicDiskMonitor diskMonitor;

  private static final long TOTAL_SPACE = 100;

  @Before
  public void start() throws IOException {
    this.fileStore = mock(FileStore.class);
    when(this.fileStore.getTotalSpace()).thenReturn(TOTAL_SPACE);
    when(this.fileStore.getUsableSpace()).thenReturn(TOTAL_SPACE);
    this.diskMonitor =
        PeriodicDiskMonitor.create(this.fileStore, 0.95, spy(new SimpleMeterRegistry()));

    Assert.assertEquals(0.0, this.diskMonitor.getDiskUtilization(), 0.0);
  }

  @After
  public void stop() {
    this.diskMonitor.stop();
  }

  @Test
  public void testMonitor() throws Exception {
    // Gate is open by default and should remain open on register
    var threshold = 0.5;
    var gate = new HysteresisGate(threshold, threshold);
    Assert.assertTrue(gate.isOpen());
    this.diskMonitor.register(gate);
    Assert.assertTrue(gate.isOpen());

    // Update the mock before start to avoid data races.
    var targetThreshold = threshold - 0.1;
    when(this.fileStore.getUsableSpace()).thenReturn(getUsableSpaceForThreshold(targetThreshold));
    this.diskMonitor.start(Duration.of(500, ChronoUnit.MILLIS));
    new Condition.Builder()
        .atMost(Duration.ofSeconds(5))
        .until(() -> this.diskMonitor.getDiskUtilization() == targetThreshold);

    // Remaining below threshold should leave gate open
    Assert.assertTrue(gate.isOpen());
  }

  @Test
  public void testCloseOpen() throws Exception {
    var closeThreshold = 0.7;
    var openThreshold = 0.5;
    var gate = new HysteresisGate(openThreshold, closeThreshold);
    this.diskMonitor.register(gate);

    // Start below close threshold
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(closeThreshold - 0.1));
    Assert.assertTrue(gate.isOpen());

    // Going above close threshold should close the gate
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(closeThreshold + 0.1));
    this.diskMonitor.update();
    Assert.assertTrue(gate.isClosed());

    // Going below close threshold but above open threshold should not open the gate
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(closeThreshold - 0.1));
    this.diskMonitor.update();
    Assert.assertTrue(gate.isClosed());

    // Going below open threshold should open the gate
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(openThreshold - 0.1));
    this.diskMonitor.update();
    Assert.assertTrue(gate.isOpen());
  }

  @Test
  public void update_noGateEverOpened_doesNotCrash() throws Exception {
    // Create a disk monitor with crash threshold at 0.8
    var crashThreshold = 0.8;
    var spyDiskMonitor =
        spy(PeriodicDiskMonitor.create(this.fileStore, crashThreshold, new SimpleMeterRegistry()));
    doNothing().when(spyDiskMonitor).crashNow();

    // Set disk utilization above crash threshold
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(crashThreshold + 0.1));
    spyDiskMonitor.update();

    // Should not crash because no gate was ever opened
    verify(spyDiskMonitor, never()).crashNow();
    spyDiskMonitor.stop();
  }

  @Test
  public void update_gateOpenedButBelowThreshold_doesNotCrash() throws Exception {
    // Create a disk monitor with crash threshold at 0.8
    var crashThreshold = 0.8;
    var spyDiskMonitor =
        spy(PeriodicDiskMonitor.create(this.fileStore, crashThreshold, new SimpleMeterRegistry()));
    doNothing().when(spyDiskMonitor).crashNow();

    // Register a gate to mark that a gate was opened
    var gate = ToggleGate.opened();
    spyDiskMonitor.register(gate);

    // Set disk utilization below crash threshold
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(crashThreshold - 0.1));
    spyDiskMonitor.update();

    // Should not crash because disk utilization is below threshold
    verify(spyDiskMonitor, never()).crashNow();
    spyDiskMonitor.stop();
  }

  @Test
  public void update_gateOpenedAndAboveThreshold_crashes() throws Exception {
    // Create a disk monitor with crash threshold at 0.8
    var crashThreshold = 0.8;
    var spyDiskMonitor =
        spy(PeriodicDiskMonitor.create(this.fileStore, crashThreshold, new SimpleMeterRegistry()));
    doNothing().when(spyDiskMonitor).crashNow();

    // Register a gate to mark that a gate was opened
    var gate = ToggleGate.opened();
    spyDiskMonitor.register(gate);

    // Set disk utilization above crash threshold
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(crashThreshold + 0.1));
    spyDiskMonitor.update();

    // Should crash because gate was opened and disk utilization is above threshold
    verify(spyDiskMonitor).crashNow();
    spyDiskMonitor.stop();
  }

  @Test
  public void update_gateOpensLaterThenThresholdExceeded_crashes() throws Exception {
    // Create a disk monitor with crash threshold at 0.8
    var crashThreshold = 0.8;
    var spyDiskMonitor =
        spy(PeriodicDiskMonitor.create(this.fileStore, crashThreshold, new SimpleMeterRegistry()));
    doNothing().when(spyDiskMonitor).crashNow();

    // Initially no gate is registered
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(crashThreshold + 0.1));
    spyDiskMonitor.update();
    verify(spyDiskMonitor, never()).crashNow();

    // Register a gate (simulating replication opening)
    var gate = ToggleGate.opened();
    spyDiskMonitor.register(gate);

    // Update again with disk still above threshold
    spyDiskMonitor.update();

    // Should crash now because gate was opened and disk utilization is above threshold
    verify(spyDiskMonitor).crashNow();
    spyDiskMonitor.stop();
  }

  @Test
  public void update_gateClosesAfterOpening_stillCrashes() throws Exception {
    // Create a disk monitor with crash threshold at 0.8
    var crashThreshold = 0.8;
    var spyDiskMonitor =
        spy(PeriodicDiskMonitor.create(this.fileStore, crashThreshold, new SimpleMeterRegistry()));
    doNothing().when(spyDiskMonitor).crashNow();

    // Register a hysteresis gate that starts open
    var gate = new HysteresisGate(0.5, 0.7);
    spyDiskMonitor.register(gate);

    // Set disk utilization to close the gate (above close threshold)
    when(this.fileStore.getUsableSpace()).thenReturn(getUsableSpaceForThreshold(0.75));
    spyDiskMonitor.update();
    Assert.assertTrue(gate.isClosed());
    verify(spyDiskMonitor, never()).crashNow();

    // Now exceed crash threshold while gate is closed
    when(this.fileStore.getUsableSpace())
        .thenReturn(getUsableSpaceForThreshold(crashThreshold + 0.1));
    spyDiskMonitor.update();

    // Should still crash because gate was opened earlier (anyGateWasOpened is true)
    verify(spyDiskMonitor).crashNow();
    spyDiskMonitor.stop();
  }

  private long getUsableSpaceForThreshold(double threshold) {
    return TOTAL_SPACE - (long) (TOTAL_SPACE * threshold);
  }
}
