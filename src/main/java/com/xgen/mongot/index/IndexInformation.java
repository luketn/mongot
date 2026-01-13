package com.xgen.mongot.index;

import com.google.common.base.Objects;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Contains high level information about the Index, namely the definition and statistics. */
public abstract sealed class IndexInformation
    permits IndexInformation.Search, IndexInformation.Vector {
  private final IndexDefinition definition;
  private final IndexStatus status;
  private final List<IndexGenerationMetrics> indexGenerationMetrics;
  private final AggregatedIndexMetrics aggregatedMetrics;

  private final Optional<IndexDetailedStatus> mainIndex;
  private final Optional<IndexDetailedStatus> stagedIndex;

  private IndexInformation(
      IndexDefinition definition,
      IndexStatus status,
      List<IndexGenerationMetrics> indexGenerationMetrics,
      AggregatedIndexMetrics aggregatedMetrics,
      Optional<IndexDetailedStatus> mainIndex,
      Optional<IndexDetailedStatus> stagedIndex) {
    this.definition = definition;
    this.status = status;
    this.indexGenerationMetrics = indexGenerationMetrics;
    this.aggregatedMetrics = aggregatedMetrics;
    this.mainIndex = mainIndex;
    this.stagedIndex = stagedIndex;
  }

  public IndexDefinition getDefinition() {
    return this.definition;
  }

  public Optional<IndexDetailedStatus> getMainIndex() {
    return this.mainIndex;
  }

  public Optional<IndexDetailedStatus> getStagedIndex() {
    return this.stagedIndex;
  }

  public IndexStatus getStatus() {
    return this.status;
  }

  public List<IndexGenerationMetrics> getIndexGenerationMetrics() {
    return this.indexGenerationMetrics;
  }

  public AggregatedIndexMetrics getAggregatedMetrics() {
    return this.aggregatedMetrics;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexInformation that = (IndexInformation) o;
    return Objects.equal(this.definition, that.definition)
        && Objects.equal(this.status, that.status)
        && Objects.equal(this.aggregatedMetrics, that.aggregatedMetrics)
        && Objects.equal(this.mainIndex, that.mainIndex)
        && Objects.equal(this.stagedIndex, that.stagedIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.definition, this.status, this.aggregatedMetrics, this.mainIndex, this.stagedIndex);
  }

  public static final class Search extends IndexInformation {

    private final Map<String, SynonymStatus> synonymStatus;

    public Search(
        SearchIndexDefinition definition,
        IndexStatus status,
        List<IndexGenerationMetrics> indexGenerationMetrics,
        AggregatedIndexMetrics aggregatedMetrics,
        Optional<IndexDetailedStatus> mainIndex,
        Optional<IndexDetailedStatus> stagedIndex,
        Map<String, SynonymStatus> synonymStatus) {
      super(definition, status, indexGenerationMetrics, aggregatedMetrics, mainIndex, stagedIndex);
      this.synonymStatus = synonymStatus;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass() || !super.equals(o)) {
        return false;
      }
      Search search = (Search) o;
      return Objects.equal(this.synonymStatus, search.synonymStatus);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), this.synonymStatus);
    }

    public Map<String, SynonymStatus> getSynonymStatus() {
      return this.synonymStatus;
    }
  }

  public static final class Vector extends IndexInformation {
    public Vector(
        VectorIndexDefinition definition,
        IndexStatus status,
        List<IndexGenerationMetrics> indexGenerationMetrics,
        AggregatedIndexMetrics aggregatedMetrics,
        Optional<IndexDetailedStatus> mainIndex,
        Optional<IndexDetailedStatus> stagedIndex) {
      super(definition, status, indexGenerationMetrics, aggregatedMetrics, mainIndex, stagedIndex);
    }
  }
}
