package com.xgen.mongot.index.lucene.directory;

import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import java.nio.file.Path;

/** A factory class to create {@link Path} for an index generation. */
public class IndexPathFactory {
  protected final Path indexPath;
  protected final Path metadataPath;
  protected final int numPartitions;

  public IndexPathFactory(Path indexPath, Path metadataPath, int numPartitions) {
    this.indexPath = indexPath;
    this.metadataPath = metadataPath;
    this.numPartitions = numPartitions;
  }

  public IndexPathFactory(IndexDirectoryHelper helper, IndexDefinitionGeneration index) {
    this(
        helper.getIndexDirectoryPath(index),
        helper.getIndexMetadataPath(index),
        index.getIndexDefinition().getNumPartitions());
  }

  public Path getIndexPartitionDataPath(int indexPartitionId) {
    // To keep old indexes compatible, we don't create subdirectories when there is only one
    // index partition.
    if (this.numPartitions > 1) {
      return this.indexPath.resolve(getIndexPartitionRelativePath(indexPartitionId));
    }
    return this.indexPath;
  }

  public Path getIndexPartitionMetadataPath(int indexPartitionId) {
    return this.metadataPath.resolve(getIndexPartitionRelativePath(indexPartitionId));
  }

  public static String getIndexPartitionDirectoryName(int indexPartitionId) {
    return Integer.toHexString(indexPartitionId);
  }

  public static Path getIndexPartitionRelativePath(int indexPartitionId) {
    return Path.of(getIndexPartitionDirectoryName(indexPartitionId));
  }

  public Path getPath() {
    return this.indexPath;
  }

  public Path getMetadataPath() {
    return this.metadataPath;
  }
}
