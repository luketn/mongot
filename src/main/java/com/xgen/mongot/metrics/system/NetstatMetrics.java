package com.xgen.mongot.metrics.system;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import java.util.List;
import oshi.SystemInfo;
import oshi.hardware.NetworkIF;

// TODO(CLOUDP-285787): investigate moving metrics (especially gauges) to MetricsFactory
public class NetstatMetrics {
  private final List<NetworkIF> networkIfList;

  static NetstatMetrics create(SystemInfo systemInfo, MeterRegistry meterRegistry) {
    List<NetworkIF> networkIfList = systemInfo.getHardware().getNetworkIFs();
    networkIfList.forEach(
        networkIf -> {
          Tags netstatTags = Tags.of("name", networkIf.getName());
          Gauge.builder("system.netstat.bytesRecv", networkIf, NetworkIF::getBytesRecv)
              .tags(netstatTags)
              .description("The Bytes Received")
              .baseUnit(BaseUnits.BYTES)
              .register(meterRegistry);
          Gauge.builder("system.netstat.bytesSent", networkIf, NetworkIF::getBytesSent)
              .tags(netstatTags)
              .description("The Bytes Sent")
              .baseUnit(BaseUnits.BYTES)
              .register(meterRegistry);
          Gauge.builder("system.netstat.collisions", networkIf, NetworkIF::getCollisions)
              .tags(netstatTags)
              .description("Packet collisions")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.inDrops", networkIf, NetworkIF::getInDrops)
              .tags(netstatTags)
              .description("Incoming/Received dropped packets")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.inErrors", networkIf, NetworkIF::getInErrors)
              .tags(netstatTags)
              .description("Input Errors")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.outErrors", networkIf, NetworkIF::getOutErrors)
              .tags(netstatTags)
              .description("The Output Errors")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.packetsRecv", networkIf, NetworkIF::getPacketsRecv)
              .tags(netstatTags)
              .description("The Packets Received")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.packetsSent", networkIf, NetworkIF::getPacketsSent)
              .tags(netstatTags)
              .description("The Packets Sent")
              .baseUnit(BaseUnits.EVENTS)
              .register(meterRegistry);
          Gauge.builder("system.netstat.speed", networkIf, NetworkIF::getSpeed)
              .tags(netstatTags)
              .description("The speed of the network interface in bits per second")
              .register(meterRegistry);
        });
    return new NetstatMetrics(networkIfList);
  }

  private NetstatMetrics(List<NetworkIF> networkIfList) {
    this.networkIfList = networkIfList;
  }

  public void update() {
    this.networkIfList.forEach(NetworkIF::updateAttributes);
  }
}
