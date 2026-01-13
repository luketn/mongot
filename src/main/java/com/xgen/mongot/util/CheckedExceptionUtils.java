package com.xgen.mongot.util;

/**
 * Unwraps a {@link RuntimeException} to propagate the underlying checked exception.
 *
 * <p>The intended use here is to expand declared exception set when overriding third party
 * libraries' classes.
 */
public class CheckedExceptionUtils {

  public static <E extends Exception> void propagateCheckedIfType(
      RuntimeException re, Class<E> expectedType) throws E {
    Throwable cause = re.getCause();
    if (expectedType.isInstance(cause)) {
      throw expectedType.cast(cause);
    } else {
      throw re;
    }
  }

  public static <E1 extends Exception, E2 extends Exception> void propagateCheckedIfType(
      RuntimeException re, Class<E1> expectedType1, Class<E2> expectedType2) throws E1, E2 {

    Throwable cause = re.getCause();
    if (expectedType1.isInstance(cause)) {
      throw expectedType1.cast(cause);
    } else if (expectedType2.isInstance(cause)) {
      throw expectedType2.cast(cause);
    } else {
      throw re;
    }
  }

  public static <E1 extends Exception, E2 extends Exception>
      void propagateUnwrappedIfTypeElseRuntime(
          Throwable throwable, Class<E1> expectedType1, Class<E2> expectedType2) throws E1, E2 {

    Throwable cause = throwable.getCause();
    if (expectedType1.isInstance(cause)) {
      throw expectedType1.cast(cause);
    } else if (expectedType2.isInstance(cause)) {
      throw expectedType2.cast(cause);
    } else {
      throw new RuntimeException(throwable);
    }
  }
}
