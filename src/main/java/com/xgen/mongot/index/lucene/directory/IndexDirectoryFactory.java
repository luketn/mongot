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

  public IndexDirectoryFactory(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      int numPartitions,
      Optional<ByteReadCollector> byteReadCollector) {
    super(indexPath, metadataPath, numPartitions);
    this.config = config;
    this.byteReadCollector = byteReadCollector;
  }

  public IndexDirectoryFactory(
      IndexDirectoryHelper helper,
      IndexDefinitionGeneration index,
      LuceneConfig config,
      Optional<ByteReadCollector> byteReadCollector) {
    this(
        helper.getIndexDirectoryPath(index),
        helper.getIndexMetadataPath(index),
        config,
        index.getIndexDefinition().getNumPartitions(),
        byteReadCollector);
  }

  public Directory create(int indexPartitionId) throws IOException {
    var indexPartitionPath = getIndexPartitionDataPath(indexPartitionId);
    FileUtils.mkdirIfNotExist(indexPartitionPath);
    FileSystemDirectory fsd = new FileSystemDirectory(indexPartitionPath, this.byteReadCollector);
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
