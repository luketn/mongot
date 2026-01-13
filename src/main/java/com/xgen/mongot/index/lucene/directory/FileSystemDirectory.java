package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

/**
 * We place the majority of index files to {@link NIOFSDirectory}, but the most
 * performance-sensitive data structures reside in {@link MMapDirectory}. Note that we avoid using
 * MMAP for everything due to a risk of affecting co-located mongod which heavily relies on the FS
 * cache. In particular, there is a concern about cache thrashing due to sequential read-ahead when
 * MMAP is used for files with random access pattern (stored fields), see additional details in
 * https://tinyurl.com/2s3ku73k and https://tinyurl.com/mtaa8asy. The second reason to not MMAP 20+
 * other file types is the limitation in max virtual memory areas (vm.max_map_count). mongod also
 * MMAPs files (2 per connection), so we might start competing for this limit if customers would
 * have tens of thousands of indexes on a single node. vm.max_map_count varies by the Atlas tier and
 * currently configured as 153852 on M10, 1048578 on M40 and limited by 4194304 on higher tiers.
 *
 * <p>A description of Lucene file format can be found here:
 * https://lucene.apache.org/core/9_2_0/core/org/apache/lucene/codecs/lucene92/package-summary.html
 */
public class FileSystemDirectory extends FileSwitchDirectory {
  private final Optional<ByteReadCollector> collector;
  private static final Set<String> MMAP_EXTENSIONS =
      Set.of(
          /* Term Index */
          "tip",
          /* Term Dictionary */
          "tim",
          /* Frequencies */
          "doc",
          /* Compound Files */
          "cfs",
          /* Norms */
          "nvd",
          /* DocValues */
          "dvd",
          /* Flat vector data file */
          "vec",
          /* Vector Index */
          "vex",
          /* Quantized flat vector data file */
          "veq",
          /* Field Data: The stored fields for the documents. That would be document ids and
           * stored source in our case, which are looked up when preparing search results. */
          "fdt");

  public FileSystemDirectory(Path path, Optional<ByteReadCollector> collector) throws IOException {
    super(MMAP_EXTENSIONS, new MMapDirectory(path), new NIOFSDirectory(path), true);
    this.collector = collector;
  }

  // OpenInputBase is used for unit test spying.
  @VisibleForTesting
  IndexInput openInputBase(String name, IOContext context) throws IOException {
    return super.openInput(name, context);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegate = openInputBase(name, context);
    return this.collector
        .<IndexInput>map(c -> new InstrumentedIndexInput(name, c, delegate))
        .orElse(delegate);
  }

  static class InstrumentedIndexInput extends IndexInput {
    private final ByteReadCollector collector;
    private final String name;
    private final IndexInput delegate;
    private final String extension;

    public InstrumentedIndexInput(
        String name, ByteReadCollector collector, IndexInput in) {
      super(name);
      this.name = name;
      this.delegate = in;
      this.extension = getExtension(name);
      this.collector = collector;
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }

    @Override
    public long getFilePointer() {
      return this.delegate.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      this.delegate.seek(pos);
    }

    @Override
    public long length() {
      return this.delegate.length();
    }

    @Override
    public IndexInput clone() {
      return new InstrumentedIndexInput(
          this.name, this.collector, this.delegate.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new InstrumentedIndexInput(
          this.name,
          this.collector,
          this.delegate.slice(sliceDescription, offset, length));
    }

    @Override
    public byte readByte() throws IOException {
      this.collector.collect(this.extension, 1);
      return this.delegate.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      this.collector.collect(this.extension, len);
      this.delegate.readBytes(b, offset, len);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      this.collector.collect(this.extension, len);
      this.delegate.readBytes(b, offset, len, useBuffer);
    }
  }
}
