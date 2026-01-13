package com.xgen.mongot.util.timers;

import com.google.common.base.Ticker;
import com.google.errorprone.annotations.Var;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.time.DateTimeException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Ticker that provides consumed CPU measurements instead of wall time.
 *
 * <p>This ticker uses a thread-specific CPU clock so users must be careful to start and stop this
 * ticker on the same thread each time. Uses across threads may return nonsensical sequences.
 *
 * <p>An implementation of CpuTicker will only be provided on platforms that can read CPU time
 * efficiently.
 */
public class CpuTicker {
  public static final Optional<Ticker> INSTANCE;

  static {
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      INSTANCE = Optional.of(new LinuxCpuTicker());
    } else {
      INSTANCE = Optional.empty();
    }
  }

  @SuppressWarnings("preview")
  private static final class LinuxCpuTicker extends Ticker {
    public static final int CLOCK_THREAD_CPUTIME_ID = 3;

    private LinuxCpuTicker() {}

    private final ValueLayout.OfLong tvSec = ValueLayout.JAVA_LONG.withName("tv_sec");
    private final ValueLayout.OfLong tvNsec = ValueLayout.JAVA_LONG.withName("tv_nsec");
    private final StructLayout timespec = MemoryLayout.structLayout(this.tvSec, this.tvNsec);
    private final long tvSecOffset =
        this.timespec.byteOffset(
            MemoryLayout.PathElement.groupElement(this.tvSec.name().orElseThrow()));
    private final long tvNsecOffset =
        this.timespec.byteOffset(
            MemoryLayout.PathElement.groupElement(this.tvNsec.name().orElseThrow()));

    private final FunctionDescriptor clockGetTimeDesc =
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS.withTargetLayout(this.timespec));
    private final MemorySegment clockGetTimeAddr =
        Linker.nativeLinker()
            .defaultLookup()
            .find("clock_gettime")
            .orElseThrow(() -> new UnsatisfiedLinkError("could not resolve clock_gettime"));

    private final MethodHandle clockGetTime =
        Linker.nativeLinker().downcallHandle(this.clockGetTimeAddr, this.clockGetTimeDesc);

    /**
     * Get the time for the specified clock ID. See clock_gettime(3) for details on clock ids.
     *
     * @return the clock value in nanoseconds.
     * @throws DateTimeException if the clock could not be read.
     */
    @Override
    public long read() {
      try (var arena = Arena.ofConfined()) {
        MemorySegment ts = arena.allocate(this.timespec);
        @Var int errno = -1;
        try {
          errno = (int) this.clockGetTime.invokeExact(CLOCK_THREAD_CPUTIME_ID, ts);
        } catch (Throwable t) {
          // NB: the invoked method is native but not JNI. It cannot throw an exception.
          throw new RuntimeException("unreachable: exception thrown from FFM native call", t);
        }
        if (errno != 0) {
          throw new DateTimeException(
              "Could not clock_gettime(" + CLOCK_THREAD_CPUTIME_ID + "): " + errno);
        }

        return TimeUnit.SECONDS.toNanos(ts.get(this.tvSec, this.tvSecOffset))
            + ts.get(this.tvNsec, this.tvNsecOffset);
      }
    }
  }
}
