package com.xgen.mongot.index.lucene.codec;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.DeflateWithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Full copy of Lucene's {@link Lucene90StoredFieldsFormat} with the difference of customized
 * BEST_SPEED compression parameters. Only 2 last constants at the end of the file have been
 * changed.
 */
public class LuceneStoredFieldsFormat extends StoredFieldsFormat {

  /** Configuration option for stored fields. */
  public enum Mode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED,
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION
  }

  /** Attribute key for compression mode. */
  public static final String MODE_KEY = Lucene90StoredFieldsFormat.class.getSimpleName() + ".mode";

  final Mode mode;

  /** Stored fields format with default options. */
  public LuceneStoredFieldsFormat() {
    this(Mode.BEST_SPEED);
  }

  /** Stored fields format with specified mode. */
  public LuceneStoredFieldsFormat(Mode mode) {
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public StoredFieldsReader fieldsReader(
      Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    String value = si.getAttribute(MODE_KEY);
    if (value == null) {
      throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
    }
    Mode mode = Mode.valueOf(value);
    return impl(mode).fieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context)
      throws IOException {
    String previous = si.putAttribute(MODE_KEY, this.mode.name());
    if (previous != null && previous.equals(this.mode.name()) == false) {
      throw new IllegalStateException(
          "found existing value for "
              + MODE_KEY
              + " for segment: "
              + si.name
              + "old="
              + previous
              + ", new="
              + this.mode.name());
    }
    return impl(this.mode).fieldsWriter(directory, si, context);
  }

  StoredFieldsFormat impl(Mode mode) {
    return switch (mode) {
      case BEST_SPEED ->
          new Lucene90CompressingStoredFieldsFormat(
              "Lucene90StoredFieldsFastData",
              BEST_SPEED_MODE,
              BEST_SPEED_BLOCK_LENGTH,
              MAX_DOCS_PER_BLOCK,
              10);
      case BEST_COMPRESSION ->
          new Lucene90CompressingStoredFieldsFormat(
              "Lucene90StoredFieldsHighData",
              BEST_COMPRESSION_MODE,
              BEST_COMPRESSION_BLOCK_LENGTH,
              4096,
              10);
    };
  }

  private static final int BEST_COMPRESSION_BLOCK_LENGTH = 10 * 48 * 1024;

  public static final CompressionMode BEST_COMPRESSION_MODE =
      new DeflateWithPresetDictCompressionMode();

  /**
   * THIS IS THE CHANGE - compression settings optimized for faster stored fields fetch time based
   * on $sort/$skip query performance tests.
   */
  private static final CompressionMode BEST_SPEED_MODE = new LZ4WithPresetDictCompressionMode();

  private static final int BEST_SPEED_BLOCK_LENGTH = 16 * 1024;

  private static final int MAX_DOCS_PER_BLOCK = 512;
}
