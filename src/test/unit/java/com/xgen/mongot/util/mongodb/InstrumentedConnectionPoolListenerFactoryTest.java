package com.xgen.mongot.util.mongodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

public class InstrumentedConnectionPoolListenerFactoryTest {
  @Test
  public void testMetersCreatedWithTags() {
    var meterRegistry = new SimpleMeterRegistry();
    var factory = new InstrumentedConnectionPoolListenerFactory(meterRegistry);
    factory.createFor("testClient");

    List<Meter> meters = meterRegistry.getMeters();

    var withExpectedTags =
        meterWithTags(Tags.of("Scope", "replication", "clientName", "testClient"));
    assertThat(
        meters,
        containsInAnyOrder(
            allOf(meterWithName("mongoClient.connectionPool.minSize"), withExpectedTags),
            allOf(meterWithName("mongoClient.connectionPool.maxSize"), withExpectedTags),
            allOf(meterWithName("mongoClient.connectionPool.connections"), withExpectedTags),
            allOf(
                meterWithName("mongoClient.connectionPool.connectionsCheckedOut"),
                withExpectedTags)));
  }

  @Test
  public void testListenerUpdatesConnectionsGauge() {
    var meterRegistry = new SimpleMeterRegistry();
    var factory = new InstrumentedConnectionPoolListenerFactory(meterRegistry);
    var listener = factory.createFor("testClient");

    List<Meter> meters = meterRegistry.getMeters();
    var gauge =
        meters.stream()
            .filter(
                meter -> meter.getId().getName().equals("mongoClient.connectionPool.connections"))
            .findFirst()
            .map(Gauge.class::cast)
            .orElseThrow();

    assertEquals(0, (int) gauge.value());

    listener.connectionCreated(
        new ConnectionCreatedEvent(
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()))));

    assertEquals(1, (int) gauge.value());

    listener.connectionClosed(
        new ConnectionClosedEvent(
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            ConnectionClosedEvent.Reason.IDLE));

    assertEquals(0, (int) gauge.value());
  }

  @Test
  public void testListenerUpdatesConnectionsCheckedOutGauge() {
    var meterRegistry = new SimpleMeterRegistry();
    var factory = new InstrumentedConnectionPoolListenerFactory(meterRegistry);
    var listener = factory.createFor("testClient");

    List<Meter> meters = meterRegistry.getMeters();
    var gauge =
        meters.stream()
            .filter(
                meter ->
                    meter
                        .getId()
                        .getName()
                        .equals("mongoClient.connectionPool.connectionsCheckedOut"))
            .findFirst()
            .map(Gauge.class::cast)
            .orElseThrow();

    assertEquals(0, (int) gauge.value());

    listener.connectionCheckedOut(
        new ConnectionCheckedOutEvent(
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 0, 0));

    assertEquals(1, (int) gauge.value());

    listener.connectionCheckedIn(
        new ConnectionCheckedInEvent(
            new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 0));

    assertEquals(0, (int) gauge.value());
  }

  @Test
  public void testListenerUpdatesConnectionPoolSizeGauge() {
    var meterRegistry = new SimpleMeterRegistry();
    var factory = new InstrumentedConnectionPoolListenerFactory(meterRegistry);
    var listener = factory.createFor("testClient");

    List<Meter> meters = meterRegistry.getMeters();
    var minSizeGauge =
        meters.stream()
            .filter(meter -> meter.getId().getName().equals("mongoClient.connectionPool.minSize"))
            .findFirst()
            .map(Gauge.class::cast)
            .orElseThrow();
    var maxSizeGauge =
        meters.stream()
            .filter(meter -> meter.getId().getName().equals("mongoClient.connectionPool.maxSize"))
            .findFirst()
            .map(Gauge.class::cast)
            .orElseThrow();

    assertEquals(0, (int) minSizeGauge.value());
    assertEquals(0, (int) maxSizeGauge.value());

    var firstServer = new ServerId(new ClusterId("cluster"), new ServerAddress());
    listener.connectionPoolCreated(
        new ConnectionPoolCreatedEvent(
            firstServer, ConnectionPoolSettings.builder().minSize(10).maxSize(20).build()));

    assertEquals(10, (int) minSizeGauge.value());
    assertEquals(20, (int) maxSizeGauge.value());

    listener.connectionPoolCreated(
        new ConnectionPoolCreatedEvent(
            new ServerId(new ClusterId("other cluster"), new ServerAddress()),
            ConnectionPoolSettings.builder().minSize(5).maxSize(10).build()));

    assertEquals(15, (int) minSizeGauge.value());
    assertEquals(30, (int) maxSizeGauge.value());

    listener.connectionPoolClosed(new ConnectionPoolClosedEvent(firstServer));

    assertEquals(5, (int) minSizeGauge.value());
    assertEquals(10, (int) maxSizeGauge.value());
  }

  @Test
  public void testConnectionPoolSizeGaugeIgnoresDuplicateEvents() {
    var meterRegistry = new SimpleMeterRegistry();
    var factory = new InstrumentedConnectionPoolListenerFactory(meterRegistry);
    var listener = factory.createFor("testClient");

    List<Meter> meters = meterRegistry.getMeters();
    var minSizeGauge =
        meters.stream()
            .filter(meter -> meter.getId().getName().equals("mongoClient.connectionPool.minSize"))
            .findFirst()
            .map(Gauge.class::cast)
            .orElseThrow();

    assertEquals(0, (int) minSizeGauge.value());

    var server = new ServerId(new ClusterId("cluster"), new ServerAddress());
    listener.connectionPoolCreated(
        new ConnectionPoolCreatedEvent(
            server, ConnectionPoolSettings.builder().minSize(10).maxSize(20).build()));

    assertEquals(10, (int) minSizeGauge.value());

    listener.connectionPoolCreated(
        new ConnectionPoolCreatedEvent(
            server, ConnectionPoolSettings.builder().minSize(1).maxSize(2).build()));

    // Value should not change
    assertEquals(10, (int) minSizeGauge.value());

    listener.connectionPoolClosed(new ConnectionPoolClosedEvent(server));
    listener.connectionPoolClosed(new ConnectionPoolClosedEvent(server));

    assertEquals(0, (int) minSizeGauge.value());
  }

  private Matcher<Meter> meterWithName(String name) {
    return new BaseMatcher<>() {
      @Override
      public boolean matches(Object o) {
        return (o instanceof Meter meter) && meter.getId().getName().equals(name);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("meter '%s'", name));
      }
    };
  }

  private Matcher<Meter> meterWithTags(Tags tags) {
    var tagMatcher = containsInAnyOrder(tags.stream().toArray());

    return new BaseMatcher<>() {
      @Override
      public boolean matches(Object o) {
        return (o instanceof Meter meter) && tagMatcher.matches(meter.getId().getTags());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("with tags matching ");
        description.appendDescriptionOf(tagMatcher);
      }
    };
  }
}
