package com.xgen.testing;

import static org.junit.Assert.assertTrue;

import com.google.common.base.Suppliers;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.junit.rules.ExternalResource;

/**
 * This class validates documents by writing each to an in-memory Index one at a time. The index is
 * automatically closed at the end of the test. Since creating indexes is expensive, consider using
 * as a {@link org.junit.ClassRule}. <br>
 * Usage:
 *
 * <pre>{@code
 * public class TestExample {
 *
 *     @ClassRule
 *     public static final LuceneIndexRule validator = new LuceneIndexRule();
 *
 *     @Test
 *     public void test() {
 *       validator.add(new Document());
 *     }
 *   }
 * }</pre>
 */
public final class LuceneIndexRule extends ExternalResource {

  private final Directory directory = new ByteBuffersDirectory();

  private final IndexWriter indexWriter;

  private final Supplier<IndexReader> indexReader;

  // Keep track of whether `before()` was called by junit framework to catch improper usage of rule.
  private boolean open;

  public LuceneIndexRule() {
    try {
      this.indexWriter =
          new IndexWriter(
              this.directory,
              getIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setCommitOnClose(true));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    // Create at most one supplier to ensure everything gets closed.
    this.indexReader =
        Suppliers.memoize(
            () -> {
              try {
                return AssertingDirectoryReader.open(this.indexWriter);
              } catch (IOException e) {
                throw new AssertionError(e);
              }
            });
  }

  /**
   * Create a new IndexWriterConfig that uses Mongot's custom Lucene Codec. The returned instance is
   * safe to modify.
   */
  public static IndexWriterConfig getIndexWriterConfig() {
    return new IndexWriterConfig().setCodec(new LuceneCodec());
  }

  /** Returns a new directory object that can be used for testing. */
  public static Directory newDirectoryForTest() {
    return new ByteBuffersDirectory();
  }

  @Override
  protected void before() {
    this.open = true;
  }

  @Override
  protected void after() {
    try {
      this.indexReader.get().close();
      this.indexWriter.close();
      this.directory.close();
      this.open = false;
    } catch (IOException e) {
      throw new AssertionError("Error closing index", e);
    }
  }

  private void checkOpen() {
    assertTrue(
        "LuceneIndexRule not setup. Must be public and annotated with @ClassRule.", this.open);
  }

  /**
   * Writes a document to the index, but do not commit to disk.
   *
   * @throws AssertionError if the document is invalid or the index is closed.
   */
  public void add(Iterable<? extends IndexableField> luceneDoc) {
    // Check open to ensure class is registered as a rule, otherwise writer will not be closed.
    checkOpen();
    try {
      this.indexWriter.addDocument(luceneDoc);
    } catch (IOException | RuntimeException e) {
      // Safely cap length of printed document. Cause will show problematic field anyway.
      String msg = "Invalid document:\n" + StringUtils.substring(luceneDoc.toString(), 0, 120);
      throw new AssertionError(msg, e);
    }
  }

  public IndexWriter getIndexWriter() {
    checkOpen();
    return this.indexWriter;
  }

  /**
   * Returns a cached "real-time" {@link IndexReader} over the {@link IndexWriter}. <br>
   * Note: This method should be called <b>AFTER</b> inserting all documents with {@link
   * #getIndexWriter()} to ensure visibility of all writes.
   */
  public IndexReader getIndexReader() {
    checkOpen();
    return this.indexReader.get();
  }
}
