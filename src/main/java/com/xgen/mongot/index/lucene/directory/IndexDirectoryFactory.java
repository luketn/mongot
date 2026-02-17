package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;

/** A factory class to create {@link Directory} according to the config. */
public class IndexDirectoryFactory extends IndexPathFactory {
  private final LuceneConfig config;
  private final Optional<ByteReadCollector> byteReadCollector;
  private final boolean prewarm;

  public IndexDirectoryFactory(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      int numPartitions,
      Optional<ByteReadCollector> byteReadCollector,
      boolean prewarm) {
    super(indexPath, metadataPath, numPartitions);
    this.config = config;
    this.byteReadCollector = byteReadCollector;
    this.prewarm = prewarm;
  }

  public IndexDirectoryFactory(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      int numPartitions,
      Optional<ByteReadCollector> byteReadCollector) {
    this(indexPath, metadataPath, config, numPartitions, byteReadCollector, false);
  }

  public IndexDirectoryFactory(
      IndexDirectoryHelper helper,
      IndexDefinitionGeneration index,
      LuceneConfig config,
      Optional<ByteReadCollector> byteReadCollector,
      boolean prewarm) {
    this(
        helper.getIndexDirectoryPath(index),
        helper.getIndexMetadataPath(index),
        config,
        index.getIndexDefinition().getNumPartitions(),
        byteReadCollector,
        prewarm);
  }

  public IndexDirectoryFactory(
      IndexDirectoryHelper helper,
      IndexDefinitionGeneration index,
      LuceneConfig config,
      Optional<ByteReadCollector> byteReadCollector) {
    this(helper, index, config, byteReadCollector, false);
  }

  public Directory create(int indexPartitionId) throws IOException {
    var indexPartitionPath = getIndexPartitionDataPath(indexPartitionId);
    FileUtils.mkdirIfNotExist(indexPartitionPath);
    FileSystemDirectory fsd = new FileSystemDirectory(indexPartitionPath, this.byteReadCollector);
    if (this.prewarm) {
      fsd.prewarmVectorFiles();
    }
    return this.config.nrtCacheEnabled()
        ? new NRTCachingDirectory(
            fsd, this.config.nrtMaxMergeSizeMb(), this.config.nrtTotalCacheSizeMb())
        : fsd;
  }

  public File createMetadata(int indexPartitionId) throws IOException {
    var indexPartitionPath = getIndexPartitionMetadataPath(indexPartitionId);
    FileUtils.mkdirIfNotExist(indexPartitionPath);
    return indexPartitionPath.toFile();
  }

  @VisibleForTesting
  public LuceneConfig getConfig() {
    return this.config;
  }
}
