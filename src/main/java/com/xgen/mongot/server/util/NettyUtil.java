package com.xgen.mongot.server.util;

import static org.apache.commons.lang3.SystemUtils.IS_OS_MAC_OSX;

import com.xgen.mongot.util.Check;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyUtil {
  private static final Logger logger = LoggerFactory.getLogger(NettyUtil.class);

  public enum SocketType {
    TCP,
    UNIX_DOMAIN
  }

  /**
   * Create {@link EventLoopGroup} with given number of threads.
   *
   * <p>Will use different implementations according to the `socketType` and OS.
   */
  public static EventLoopGroup createEventLoopGroup(SocketType socketType, int numThreads) {
    return switch (socketType) {
      case TCP -> new NioEventLoopGroup(numThreads);
      case UNIX_DOMAIN ->
          IS_OS_MAC_OSX
              ? new KQueueEventLoopGroup(numThreads)
              : new EpollEventLoopGroup(numThreads);
    };
  }

  /**
   * Create {@link EventLoopGroup} with default number of threads.
   *
   * <p>Will use different implementations according to the `socketType` and OS.
   */
  public static EventLoopGroup createEventLoopGroup(SocketType socketType) {
    return switch (socketType) {
      case TCP -> new NioEventLoopGroup();
      case UNIX_DOMAIN -> IS_OS_MAC_OSX ? new KQueueEventLoopGroup() : new EpollEventLoopGroup();
    };
  }

  /**
   * Get the class of server channel according to the `socketType` and OS.
   *
   * <p>This is used when creating Netty servers.
   */
  public static Class<? extends io.netty.channel.ServerChannel> getServerChannelType(
      SocketType socketType) {
    return switch (socketType) {
      case TCP -> NioServerSocketChannel.class;
      case UNIX_DOMAIN ->
          IS_OS_MAC_OSX
              ? KQueueServerDomainSocketChannel.class
              : EpollServerDomainSocketChannel.class;
    };
  }

  /**
   * Get the class of client channel according to the `socketType` and OS.
   *
   * <p>This is used when creating Netty clients.
   */
  public static Class<? extends io.netty.channel.Channel> getClientChannelType(
      SocketType socketType) {
    return switch (socketType) {
      case TCP -> NioSocketChannel.class;
      case UNIX_DOMAIN ->
          IS_OS_MAC_OSX ? KQueueDomainSocketChannel.class : EpollDomainSocketChannel.class;
    };
  }

  /**
   * Get {@link SocketType} according to the {@link SocketAddress}.
   *
   * <p>This is used when creating Netty clients and servers.
   */
  public static SocketType getSocketType(SocketAddress socketAddress) {
    if (socketAddress instanceof InetSocketAddress) {
      return SocketType.TCP;
    }
    if (socketAddress instanceof DomainSocketAddress) {
      return SocketType.UNIX_DOMAIN;
    }
    logger.error("Unexpected SocketAddress type: {}", socketAddress.getClass().getName());
    return Check.unreachable("Unexpected SocketAddress type");
  }
}
