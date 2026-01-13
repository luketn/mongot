package com.xgen.mongot.metrics.system;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.software.os.OSFileStore;

// TODO(CLOUDP-285787): investigate moving metrics (especially gauges) to MetricsFactory
public class DiskMetrics {
  private final List<HWDiskStore> hwDiskStoreList;
  private final List<OSFileStore> fileStoreList;
  private final Optional<DiskMetrics.DataPathDiskMetrics> dataPathDiskMetrics;
  private static final Logger LOG = LoggerFactory.getLogger(DiskMetrics.class);

  static DiskMetrics create(SystemInfo systemInfo, MeterRegistry meterRegistry, Path dataPath) {
    List<HWDiskStore> hwDiskStoreList =
        systemInfo.getHardware().getDiskStores().stream()
            .filter(
                // Avoid collecting metrics when the disk is not used.
                // A similar logic in MongoDB:
                // https://github.com/mongodb/mongo/blob/v7.0/src/mongo/util/procparser.cpp#L595
                hwDiskStore -> hwDiskStore.getReads() > 0 || hwDiskStore.getWrites() > 0)
            .collect(Collectors.toList());
    hwDiskStoreList.forEach(
        hwDiskStore -> {
          Tags diskTags = Tags.of("name", hwDiskStore.getName());
          Gauge.builder(
                  "system.disk.currentQueueLength", hwDiskStore, HWDiskStore::getCurrentQueueLength)
              .tags(diskTags)
              .description("The length of the disk queue (#I/O's in progress)")
              .baseUnit(BaseUnits.TASKS)
              .register(meterRegistry);
          Gauge.builder("system.disk.readBytes", hwDiskStore, HWDiskStore::getReadBytes)
              .tags(diskTags)
              .description("The number of bytes read from the disk")
              .baseUnit(BaseUnits.BYTES)
              .register(meterRegistry);
          Gauge.builder("system.disk.reads", hwDiskStore, HWDiskStore::getReads)
              .tags(diskTags)
              .description("The number of reads from the disk")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.disk.transferTime", hwDiskStore, HWDiskStore::getTransferTime)
              .tags(diskTags)
              .description("The time spent reading or writing")
              .baseUnit(BaseUnits.MILLISECONDS)
              .register(meterRegistry);
          Gauge.builder("system.disk.writeBytes", hwDiskStore, HWDiskStore::getWriteBytes)
              .tags(diskTags)
              .description("The number of bytes written to the disk")
              .baseUnit(BaseUnits.BYTES)
              .register(meterRegistry);
          Gauge.builder("system.disk.writes", hwDiskStore, HWDiskStore::getWrites)
              .tags(diskTags)
              .description("The number of writes to the disk")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
        });

    List<OSFileStore> fileStoreList =
        systemInfo.getOperatingSystem().getFileSystem().getFileStores();
    Gauge.builder(
            "system.disk.space.total",
            () ->
                fileStoreList.stream()
                    .map(OSFileStore::getTotalSpace)
                    .mapToLong(Long::longValue)
                    .sum())
        .description("Total disk space from file system")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);
    Gauge.builder(
            "system.disk.space.free",
            () ->
                fileStoreList.stream()
                    .map(OSFileStore::getFreeSpace)
                    .mapToLong(Long::longValue)
                    .sum())
        .description("Free disk space from file system")
        .baseUnit(BaseUnits.BYTES)
        .register(meterRegistry);

    return new DiskMetrics(hwDiskStoreList, fileStoreList, dataPath, meterRegistry);
  }

  private DiskMetrics(
      List<HWDiskStore> hwDiskStoreList,
      List<OSFileStore> fileStoreList,
      Path dataPath,
      MeterRegistry meterRegistry) {
    this.hwDiskStoreList = hwDiskStoreList;
    this.fileStoreList = fileStoreList;
    this.dataPathDiskMetrics = DataPathDiskMetrics.create(dataPath, meterRegistry);
  }

  public void update() {
    this.hwDiskStoreList.forEach(HWDiskStore::updateAttributes);
    this.fileStoreList.forEach(OSFileStore::updateAttributes);
    this.dataPathDiskMetrics.ifPresent(DataPathDiskMetrics::update);
  }

  private static class DataPathDiskMetrics {
    final AtomicLong freeDiskSpace;
    final AtomicLong totalDiskSpace;
    final FileStore fileStore;

    private static Optional<DataPathDiskMetrics> create(
        Path dataPath, MeterRegistry meterRegistry) {
      try {
        AtomicLong totalDiskSpace = new AtomicLong(0);
        AtomicLong freeDiskSpace = new AtomicLong(0);
        FileStore fileStore = Files.getFileStore(dataPath);
        totalDiskSpace.set(fileStore.getTotalSpace());
        freeDiskSpace.set(fileStore.getUsableSpace());
        Gauge.builder("system.disk.space.data.path.total", totalDiskSpace, AtomicLong::get)
            .description("Total disk space from data path")
            .baseUnit(BaseUnits.BYTES)
            .register(meterRegistry);

        Gauge.builder("system.disk.space.data.path.free", freeDiskSpace, AtomicLong::get)
            .description("Free disk space from data path")
            .baseUnit(BaseUnits.BYTES)
            .register(meterRegistry);
        return Optional.of(new DataPathDiskMetrics(freeDiskSpace, totalDiskSpace, fileStore));
      } catch (IOException e) {
        LOG.atWarn()
            .addKeyValue("exceptionMessage", e.getMessage())
            .log("Error initializing data path disk metrics");
        return Optional.empty();
      }
    }

    private DataPathDiskMetrics(
        AtomicLong freeDiskSpace, AtomicLong totalDiskSpace, FileStore fileStore) {
      this.totalDiskSpace = totalDiskSpace;
      this.freeDiskSpace = freeDiskSpace;
      this.fileStore = fileStore;
    }

    private void update() {
      try {
        this.totalDiskSpace.set(this.fileStore.getTotalSpace());
        this.freeDiskSpace.set(this.fileStore.getUsableSpace());
      } catch (IOException e) {
        LOG.atWarn()
            .addKeyValue("exceptionMessage", e.getMessage())
            .log("Error retrieving disk space information");
      }
    }
  }
}
