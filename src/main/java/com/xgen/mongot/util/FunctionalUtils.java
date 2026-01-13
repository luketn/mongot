package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.util.function.Consumer;

public class FunctionalUtils {

  /**
   * Returns a no-operation {@link Consumer} that accepts an input but performs no action.
   *
   * <p>This is useful when a {@link Consumer} is required but no actual operation needs to be
   * performed on the input. The returned {@link Consumer} will silently accept any input and do
   * nothing with it.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Consumer<String> noop = nopConsumer();
   * noop.accept("test"); // Does nothing
   * }</pre>
   *
   * @param <T> the type of the input to the {@link Consumer}
   * @return a {@link Consumer} that ignores its input and performs no action
   */
  public static <T> Consumer<T> nopConsumer() {
    return ignored -> {};
  }

  public static <T, E extends Exception> T getOrDefaultIfThrows(
      CheckedSupplier<T, E> function, Class<E> expectedException, T defaultValue) {
    try {
      return function.get();
    } catch (Exception e) {
      if (expectedException.isInstance(e)) {
        return defaultValue;
      }
      throw new IllegalStateException(e);
    }
  }
}
