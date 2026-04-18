package com.xgen.mongot.server.command.registry;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.CommandFactoryMarker;
import com.xgen.mongot.server.command.builtin.BuildInfoCommand;
import com.xgen.mongot.server.command.builtin.HelloCommand;
import com.xgen.mongot.server.command.builtin.IsMasterCommand;
import com.xgen.mongot.server.command.builtin.PingCommand;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CommandRegistry {
  private static final String SEARCH_COMMAND_NAME = "search";
  private static final Duration[] SEARCH_COMMAND_LATENCY_BUCKETS = {
    micros(50),
    micros(75),
    micros(100),
    micros(125),
    micros(150),
    micros(200),
    micros(250),
    micros(300),
    micros(400),
    micros(500),
    micros(750),
    millis(1),
    millis(2),
    millis(3),
    millis(4),
    millis(6),
    millis(8),
    millis(12),
    millis(16),
    millis(24),
    millis(32),
    millis(48),
    millis(64),
    millis(96),
    millis(128),
    millis(256),
    millis(512),
    Duration.ofSeconds(1)
  };

  private final Map<String, CommandRegistration> commands;
  private final MetricsFactory commandMetricsFactory;

  private CommandRegistry(MetricsFactory commandMetricsFactory) {
    this.commands = new ConcurrentHashMap<>();
    this.commandMetricsFactory = commandMetricsFactory;
  }

  public static CommandRegistry create(MeterRegistry meterRegistry) {
    MetricsFactory metricsFactory = new MetricsFactory("command", meterRegistry);
    CommandRegistry commandRegistry = new CommandRegistry(metricsFactory);
    registerBuiltInCommands(commandRegistry);
    return commandRegistry;
  }

  private static void registerBuiltInCommands(CommandRegistry commandRegistry) {
    commandRegistry.registerInsecureCommand(
        BuildInfoCommand.ALT_NAME, BuildInfoCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(HelloCommand.NAME, HelloCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(IsMasterCommand.NAME, IsMasterCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(
        IsMasterCommand.ALT_NAME, IsMasterCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(PingCommand.NAME, PingCommand.FACTORY, false);
  }

  public CommandRegistry registerCommand(
      String commandName, CommandFactoryMarker factory, boolean detailedStats) {
    this.commands.put(
        commandName,
        new CommandRegistration(
            commandName, factory, true, this.commandMetricsFactory, detailedStats));
    return this;
  }

  public CommandRegistry registerInsecureCommand(
      String commandName, CommandFactoryMarker factory, boolean detailedStats) {
    this.commands.put(
        commandName,
        new CommandRegistration(
            commandName, factory, false, this.commandMetricsFactory, detailedStats));
    return this;
  }

  public CommandRegistration getCommandRegistration(String commandName) {
    CommandRegistration reg = this.commands.get(commandName);
    if (reg == null) {
      throw new IllegalArgumentException("no command registered for " + commandName);
    }
    return reg;
  }

  public static class CommandRegistration {
    public final CommandFactoryMarker factory;
    public final boolean isSecure;
    public final Timer totalTimer;
    public final Optional<Timer> serializationTimer;
    public final Counter failureCounter;

    private CommandRegistration(
        String commandName,
        CommandFactoryMarker factory,
        boolean isSecure,
        MetricsFactory commandMetricsFactory,
        boolean detailedStats) {
      this.factory = factory;
      this.isSecure = isSecure;
      // To avoid triggering lookups for each command, the metric is cached in this class.
      if (detailedStats) {
        this.totalTimer = totalLatencyTimer(commandName, commandMetricsFactory, true);
        this.serializationTimer =
            Optional.of(
                commandMetricsFactory.timer(
                    String.format("%sCommandSerializationLatency", commandName),
                    Tags.of(
                        "timeUnit",
                        Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.MICROSECONDS)),
                    0.5,
                    0.75,
                    0.9,
                    0.99));
      } else {
        // NB: no percentiles
        this.totalTimer = totalLatencyTimer(commandName, commandMetricsFactory, false);
        this.serializationTimer = Optional.empty();
      }
      this.failureCounter =
          commandMetricsFactory.counter(String.format("%sCommandFailure", commandName));
    }
  }

  private static Duration micros(long micros) {
    return Duration.ofNanos(micros * 1_000);
  }

  private static Duration millis(long millis) {
    return Duration.ofMillis(millis);
  }

  private static Timer totalLatencyTimer(
      String commandName, MetricsFactory commandMetricsFactory, boolean detailedStats) {
    String metricName = String.format("%sCommandTotalLatency", commandName);
    if (detailedStats && SEARCH_COMMAND_NAME.equals(commandName)) {
      return commandMetricsFactory.histogramTimer(metricName, SEARCH_COMMAND_LATENCY_BUCKETS);
    }
    if (detailedStats) {
      return commandMetricsFactory.timer(metricName);
    }
    return commandMetricsFactory.timer(metricName, Tags.empty());
  }
}
