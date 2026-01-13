package com.xgen.mongot.index.lucene.facet;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Class used to create an ordinal map, used for string faceting, from token type fields. This class
 * is forked from {@link org.apache.lucene.facet.StringDocValuesReaderState}, but extends from
 * {@link SortedSetDocValuesReaderState} since it's currently better supported by Lucene faceting
 * implementations. This class assumes that each facet field is held in its own Lucene field, and
 * the entire string stored at each ordinal is the facet value for that field.
 *
 * <p>We should try to remove this custom implementation when/if Lucene extends support for {@link
 * org.apache.lucene.facet.StringDocValuesReaderState} to concurrent string faceting, drill
 * sideways/down, etc.
 */
public class TokenSsdvFacetState extends SortedSetDocValuesReaderState {
  final IndexReader reader;
  final String luceneFieldName;
  final Optional<OrdinalMap> ordinalMap;
  final int cardinality;

  /**
   * Instantiates a new TokenSSDVFacetState
   *
   * @param reader IndexReader tied to this FacetState. This class must be re-created if a new
   *     IndexReader is created.
   * @param luceneFieldName lucene field path corresponding to the ordinal state being stored here
   * @param ordinalMap ordinal map for the lucene field, mapping segment-wise ordinals to global
   *     ordinals. Empty if 0 or 1 segment is present, since single segment does not require global
   *     ordinal.
   * @param cardinality cardinality of the field
   */
  public TokenSsdvFacetState(
      IndexReader reader,
      String luceneFieldName,
      Optional<OrdinalMap> ordinalMap,
      int cardinality) {
    this.reader = reader;
    this.luceneFieldName = luceneFieldName;
    this.ordinalMap = ordinalMap;
    this.cardinality = cardinality;
  }

  public static Optional<TokenSsdvFacetState> create(
      IndexReader reader, String luceneFieldName, Optional<Integer> cardinalityLimit)
      throws IOException, TokenFacetsCardinalityLimitExceededException {
    SortedSetDocValues docValues = getDocValues(reader, luceneFieldName, Optional.empty());

    if (docValues.getValueCount() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s has cardinality %d. Lucene can only handle cardinality < Integer.MAX_VALUE",
              luceneFieldName, docValues.getValueCount()));
    }

    int cardinality = (int) docValues.getValueCount();

    if (cardinality == 0) {
      return Optional.empty();
    }
    if (cardinalityLimit.map(limit -> cardinality > limit).orElse(false)) {
      throw new TokenFacetsCardinalityLimitExceededException(
          String.format(
              "Field %s has cardinality %d. This cluster cannot facet on"
                  + " fields with >%s cardinality. See documentation on shared tier"
                  + " cluster limitations for more information.",
              luceneFieldName, docValues.getValueCount(), cardinalityLimit.get()));
    }
    if (docValues instanceof MultiDocValues.MultiSortedSetDocValues multiDocValues) {
      return Optional.of(
          new TokenSsdvFacetState(
              reader, luceneFieldName, Optional.of(multiDocValues.mapping), cardinality));
    }
    return Optional.of(
        new TokenSsdvFacetState(reader, luceneFieldName, Optional.empty(), cardinality));
  }

  /** Return top-level doc values. */
  @Override
  public SortedSetDocValues getDocValues() throws IOException {
    return getDocValues(this.reader, this.luceneFieldName, this.ordinalMap);
  }

  /** Logic forked from {@link org.apache.lucene.facet.StringDocValuesReaderState} */
  private static SortedSetDocValues getDocValues(
      IndexReader reader, String luceneFieldName, Optional<OrdinalMap> ordinalMap)
      throws IOException {
    List<LeafReaderContext> leaves = reader.leaves();
    int leafCount = leaves.size();
    if (leafCount == 0) {
      return DocValues.emptySortedSet();
    }

    if (leafCount == 1) {
      return DocValues.getSortedSet(leaves.getFirst().reader(), luceneFieldName);
    }
    SortedSetDocValues[] docValues = new SortedSetDocValues[leafCount];
    int[] starts = new int[leafCount + 1];
    @Var long cost = 0;
    for (int i = 0; i < leafCount; i++) {
      LeafReaderContext context = reader.leaves().get(i);
      docValues[i] = DocValues.getSortedSet(context.reader(), luceneFieldName);
      starts[i] = context.docBase;
      cost += docValues[i].cost();
    }
    starts[leafCount] = reader.maxDoc();
    IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
    IndexReader.CacheKey owner = cacheHelper == null ? null : cacheHelper.getKey();
    OrdinalMap map =
        ordinalMap.isPresent()
            ? ordinalMap.get()
            : OrdinalMap.build(owner, docValues, PackedInts.DEFAULT);

    return new MultiDocValues.MultiSortedSetDocValues(docValues, starts, map, cost);
  }

  /** Indexed field we are reading. */
  @Override
  public String getField() {
    return this.luceneFieldName;
  }

  /** Returns top-level index reader. */
  @Override
  public IndexReader getReader() {
    return this.reader;
  }

  /** Number of unique labels. */
  @Override
  public int getSize() {
    return this.cardinality;
  }

  /** Returns the associated facet config. */
  @Override
  public FacetsConfig getFacetsConfig() {
    FacetsConfig facetsConfig = new FacetsConfig();
    facetsConfig.setHierarchical(this.luceneFieldName, false);
    facetsConfig.setMultiValued(this.luceneFieldName, true);
    facetsConfig.setIndexFieldName(this.luceneFieldName, this.luceneFieldName);
    facetsConfig.setDrillDownTermsIndexing(
        this.luceneFieldName, FacetsConfig.DrillDownTermsIndexing.ALL);
    return facetsConfig;
  }

  /**
   * Returns the {@link OrdRange} for this dimension.
   *
   * @param luceneFieldName the name of the facet field used for this ordinal map. Only used for
   *     verification in this implementation.
   */
  @Override
  public OrdRange getOrdRange(String luceneFieldName) {
    if (!luceneFieldName.equals(this.luceneFieldName)) {
      throw new IllegalArgumentException(
          String.format(
              "queried dim must be equal to field name. Expected %s, but got %s",
              this.luceneFieldName, luceneFieldName));
    }
    return new OrdRange(0, this.cardinality - 1);
  }

  /** Returns mapping from prefix to {@link OrdRange}. */
  @Override
  public Map<String, OrdRange> getPrefixToOrdRange() {
    return Map.of(this.luceneFieldName, getOrdRange(this.luceneFieldName));
  }

  /** Only used for hierarchical facets, not supported in this implementation */
  @Override
  public DimTree getDimTree(String luceneFieldName) {
    throw new UnsupportedOperationException("Hierarchical facets not supported");
  }

  /** The only dimension indexed is the lucene field */
  @Override
  public Iterable<String> getDims() {
    return List.of(this.luceneFieldName);
  }

  /** Return the memory usage of the ordinal map */
  @Override
  public long ramBytesUsed() {
    return this.ordinalMap.map(OrdinalMap::ramBytesUsed).orElse(0L);
  }

  // Used to check equality between different index readers. Only field is persisted, everything
  // else can be different.
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TokenSsdvFacetState that = (TokenSsdvFacetState) o;
    return Objects.equals(getField(), that.getField());
  }

  @Override
  public int hashCode() {
    return this.getField().hashCode();
  }
}
