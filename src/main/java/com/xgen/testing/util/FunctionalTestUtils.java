package com.xgen.testing.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.function.Function;

public class FunctionalTestUtils {
  /**
   * Wraps a {@link CheckedFunction} that throws a checked exception and converts it into a standard
   * {@link Function}, rethrowing any checked exceptions as unchecked {@link RuntimeException}s.
   *
   * <p>This utility method is useful when dealing with functional interfaces (e.g., {@link
   * java.util.Optional#map(Function)}) that do not allow checked exceptions to be thrown directly.
   * The checked exception is caught and rethrown as a {@link RuntimeException}, allowing it to be
   * handled or propagated later in the execution.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // An example method that throws a checked exception.
   * public static int bsonParseInt(str String) throws BsonParseException {
   *   try {
   *     return Integer.parse(str);
   *   } catch (Exception e) {
   *     throw new BsonParseException(e);
   *   }
   * }
   *
   * // Suppressing the checked exception using the unchecked wrapper
   * Optional<String> optionalStr = Optional.of("123");
   * Optional<Integer> optionalInt = optionalStr.map(unchecked(bsonParseInt));
   * System.out.println(optionalInt);  // Output: Optional[123]
   *
   * // Example of throwing an exception
   * Optional<String> invalidStr = Optional.of("invalid");
   * // This will throw RuntimeException wrapping BsonParseException
   * invalidStr.map(unchecked(bsonParseInt));
   * }</pre>
   *
   * @param <T> the type of the input to the function
   * @param <R> the type of the result of the function
   * @param <E> the type of the exception thrown by the checked function
   * @param function the {@link CheckedFunction} that may throw a checked exception
   * @return a {@link Function} that wraps the {@link CheckedFunction}, rethrowing any checked
   *     exceptions as {@link RuntimeException}s
   */
  public static <T, R, E extends Throwable> Function<T, R> unchecked(
      CheckedFunction<T, R, E> function) {
    return t -> {
      try {
        return function.apply(t);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }
}
