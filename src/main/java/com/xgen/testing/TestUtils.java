package com.xgen.testing;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.util.Check.checkState;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.testing.EqualsTester;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Assert;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

public class TestUtils {

  public static final float EPSILON = 1e-4f;

  /**
   * Returns a TemporaryFolder that can be used by tests that need to interact with the filesystem.
   */
  public static TemporaryFolder getTempFolder() throws IOException {
    String testTempDirPath = System.getenv("TEST_TMPDIR");
    Check.stateNotNull(
        testTempDirPath,
        "TEST_TMPDIR environment variable must be set (should be set automatically by Bazel)");

    File tempTestDir = Paths.get(testTempDirPath).toFile();
    checkState(tempTestDir.exists(), "TEST_TMPDIR %s does not exist", testTempDirPath);
    TemporaryFolder folder = new TemporaryFolder(tempTestDir);
    folder.create();
    return folder;
  }

  public static Path getTempFolderPath() throws IOException {
    return getTempFolder().getRoot().toPath();
  }

  /** Assert that collection contains an element. */
  public static <E> void assertContains(Collection<E> collection, E element) {
    if (!collection.contains(element)) {
      throw new AssertionError(
          String.format("element '%s' not contained in collection: '%s'", element, collection));
    }
  }

  /**
   * For every object supplier provided, creates two objects and checks that they are equals() and
   * have the same hashCode().
   *
   * <p>Note that each time an objectSupplier is invoked it must return a different object, so we
   * are not just checking obj1 == obj2.
   */
  public static <T, E extends Exception> void assertEqualityGroups(
      List<CheckedSupplier<T, E>> objectSuppliers) throws E {
    var equalityTester = new EqualsTester();
    for (var objectSupplier : objectSuppliers) {
      var object1 = objectSupplier.get();
      var object2 = objectSupplier.get();
      Assert.assertNotSame(
          "objects supplied by the object supplier were the same object, must be different objects",
          object1,
          object2);

      equalityTester.addEqualityGroup(object1, object2);
    }

    equalityTester.testEquals();
  }

  @SafeVarargs
  public static <T, E extends Exception> void assertEqualityGroups(
      CheckedSupplier<T, E>... objectSuppliers) throws E {
    assertEqualityGroups(Arrays.asList(objectSuppliers));
  }

  /**
   * Catches any thrown exception and downgrades it to a RuntimeExceptions and re-throws.
   *
   * <p>Useful if you need to write code in a test that can throw a checked exception which does not
   * need to be gracefully handled and can simply be bubbled up as a runtime exception to the test
   * runner to fail the test if thrown.
   *
   * <p>For example, many concurrency primitives that block throw an InterruptedException, which can
   * be tedious to handle in a lambda designed to be running in a thread:
   *
   * <pre>{@code
   * var firstLinePrinted = new CountDownLatch(1);
   *
   * var thread1 = new Thread( () -> { downgradeCheckedExceptions(() -> firstLinePrinted.await());
   * System.out.println("second line"); });
   * thread1.start();
   *
   * var thread2 = new Thread( () -> {
   *  System.out.println("first line");
   *  firstLinePrinted.countDown();
   * });
   * thread2.start();
   * }</pre>
   */
  public static <E extends Exception> void downgradeCheckedExceptions(CheckedRunnable<E> runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Catches any thrown exception and downgrades it to a RuntimeExceptions and re-throws, otherwise
   * returning the value returned by the supplier.
   *
   * <p>Useful if you need to write code in a test that can throw a checked exception which does not
   * need to be gracefully handled and can simply be bubbled up as a runtime exception to the test
   * runner to fail the test if thrown.
   *
   * <p>For example, many concurrency primitives that block throw an InterruptedException, which can
   * be tedious to handle in a stream: <code> var executor = Executors.newCachedThreadPool();
   * var numbers = IntStream.range(0, 10) .mapToObj(i -> CompletableFuture.supplyAsync(() -> i,
   * executor)) .map(future -> downgradeCheckedExceptions(() -> future.get()))
   * .collect(Collectors.toList());
   * </code>
   */
  public static <T, E extends Exception> T downgradeCheckedExceptions(
      CheckedSupplier<T, E> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Throws {@link AssertionError} if any element of {@code scoreDocs} has a negative or NaN score.
   */
  public static void assertHasValidScores(ScoreDoc[] scoreDocs) {
    for (ScoreDoc scoreDoc : scoreDocs) {
      assertWithMessage("ScoreDoc(%s) has missing score", scoreDoc).that(scoreDoc.score).isNotNaN();
      assertWithMessage("ScoreDoc(%s) has negative score", scoreDoc)
          .that(scoreDoc.score)
          .isAtLeast(0);
    }
  }

  /**
   * Throws an {@link AssertionError} if {@code scoreDocs} doesn't contain exactly the doc IDs
   * specified in {@code expectedDocs} in their declared order.
   */
  public static void assertHasDocIds(ScoreDoc[] scoreDocs, Integer... expectedDocs) {
    int[] actual = Arrays.stream(scoreDocs).mapToInt(s -> s.doc).toArray();
    assertThat(actual).asList().containsExactlyElementsIn(expectedDocs).inOrder();
  }

  /**
   * Asserts that running {@code action} throws an instance of type {@code T} such that its message
   * contains {@code msg} as a substring.
   */
  public static <T extends Throwable> T assertThrows(
      String msg, Class<T> expectedException, ThrowingRunnable action) {
    T ex = Assert.assertThrows(expectedException, action);
    assertThat(ex).hasMessageThat().contains(msg);
    return ex;
  }

  /**
   * Obtains a {@link org.slf4j.Logger} associated with the class {@code c}. Casts it to a {@link
   * ch.qos.logback.classic.Logger}, such that you can read log events sent through the logger.
   * Usually works in tandem with {@link TestUtils#getLogEvents(Logger)}, such that you can get the
   * class's logger's messages.
   *
   * @param c The class which you wish to get its logger.
   * @return the class {@code c} logger.
   */
  public static Logger getClassLogger(Class<?> c) {
    return (Logger) LoggerFactory.getLogger(c);
  }

  /**
   * Takes in a {@link ch.qos.logback.classic.Logger} and gets a list of its log messages. Usually
   * works in tandem with {@link TestUtils#getClassLogger(Class)}, by accepting the logger that it
   * outputs.
   *
   * <p>The list will have all log messages that were sent through logger {@code l} <b>after</b>
   * {@code getLogEvents()} is called. For instance:
   *
   * <pre>{@code
   * MyClass.badStuff(); // logs a warning
   * List<ILoggingEvent> list = TestUtils.getLogEvents(MyClassLogger);
   * MyClass.badStuff(); // logs a warning
   * assertEquals(1, list.size()); // only the warning after getLogEvents() was called is found
   * }</pre>
   *
   * @param l The logger which has the desired messages.
   * @return a list of the {@link ILoggingEvent}s which relate to the log messages routed through
   *     {@code l}.
   */
  public static List<ILoggingEvent> getLogEvents(Logger l) {
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    l.addAppender(listAppender);
    List<ILoggingEvent> list = listAppender.list;
    return list;
  }

  /**
   * Create an array of "weird" double values that present edge cases for tests.
   *
   * <p>This includes infinities, NaNs, extrema, signed zero, and sub-normals. This function returns
   * a new instance with every invocation so that callers can safely modify the aray.
   */
  public static double[] createWeirdDoubles() {
    return new double[] {
      Double.NaN,
      Double.NEGATIVE_INFINITY,
      -Double.MAX_VALUE,
      Math.nextUp(-Double.MAX_VALUE),
      -1.0,
      -Double.MIN_NORMAL,
      -Double.MIN_VALUE,
      -0.0,
      0.0,
      Double.MIN_VALUE,
      Double.MIN_NORMAL,
      1.0,
      Math.nextDown(Double.MAX_VALUE),
      Double.MAX_VALUE,
      Double.POSITIVE_INFINITY,
    };
  }
}
