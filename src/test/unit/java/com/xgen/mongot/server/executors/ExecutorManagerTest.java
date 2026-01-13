package com.xgen.mongot.server.executors;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.util.NettyUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.EventLoopGroup;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ExecutorManagerTest {
  @Parameterized.Parameters(name = "socketType={0}")
  public static List<NettyUtil.SocketType> serverTypes() {
    return List.of(NettyUtil.SocketType.TCP, NettyUtil.SocketType.UNIX_DOMAIN);
  }

  private final NettyUtil.SocketType socketType;

  public ExecutorManagerTest(NettyUtil.SocketType socketType) {
    this.socketType = socketType;
  }

  private static Command createExampleAsyncCommand() {
    return new Command() {
      @Override
      public String name() {
        return "";
      }

      @Override
      public BsonDocument run() {
        return new BsonDocument();
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.ASYNC;
      }
    };
  }

  @Test
  public void testExecution() throws ExecutionException, InterruptedException {
    ExecutorManager manager = new ExecutorManager(new SimpleMeterRegistry());

    // Schedule a Command in commandExecutor.
    CompletableFuture<BsonDocument> commandExecutorFuture =
        manager.commandExecutor.execute(createExampleAsyncCommand());

    // Schedule a Runnable in bossGroup.
    CompletableFuture<Void> bossGroupFuture = new CompletableFuture<Void>();
    manager
        .getEventLoopGroup(this.socketType, ExecutorManager.EventLoopGroupType.BOSS)
        .execute(() -> bossGroupFuture.complete(null));

    // Schedule a Runnable in workerGroup.
    CompletableFuture<Void> workerGroupFuture = new CompletableFuture<Void>();
    manager
        .getEventLoopGroup(this.socketType, ExecutorManager.EventLoopGroupType.WORKER)
        .execute(() -> workerGroupFuture.complete(null));

    // Block until the Command and the Runnables complete execution.
    commandExecutorFuture.get();
    bossGroupFuture.get();
    workerGroupFuture.get();

    manager.shutdown();
  }

  @Test
  public void testShutdown() throws InterruptedException {
    ExecutorManager manager = new ExecutorManager(new SimpleMeterRegistry());
    EventLoopGroup bossGroup =
        manager.getEventLoopGroup(this.socketType, ExecutorManager.EventLoopGroupType.BOSS);
    EventLoopGroup workerGroup =
        manager.getEventLoopGroup(this.socketType, ExecutorManager.EventLoopGroupType.BOSS);
    manager.shutdown();

    // All executors shutdown successfully. Cannot schedule any Commands and Runnables.
    Assert.assertThrows(
        RejectedExecutionException.class,
        () -> manager.commandExecutor.execute(createExampleAsyncCommand()).get());
    Assert.assertThrows(
        RejectedExecutionException.class,
        () ->
            bossGroup.execute(
                () -> {
                  // Do nothing.
                }));
    Assert.assertThrows(
        RejectedExecutionException.class,
        () ->
            workerGroup.execute(
                () -> {
                  // Do nothing.
                }));
  }
}
