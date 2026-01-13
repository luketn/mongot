package com.xgen.mongot.index.lucene;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public interface SortValuesProvider {

  /** Returns the Lucene sort fields from the given {@link TopDocs} if present */
  Optional<SortField[]> sortFields(TopDocs topDocs);

  /** Extracts BSON values used for sorting from the given {@link ScoreDoc}. */
  Optional<List<BsonValue>> sortValues(ScoreDoc scoreDoc);

  final class RealSortValuesProvider implements SortValuesProvider {
    @Override
    public Optional<SortField[]> sortFields(TopDocs topDocs) {
      if (topDocs instanceof TopFieldDocs topFieldDocs) {
        return Optional.of(topFieldDocs.fields);
      }
      throw new IllegalStateException("ScoreDoc must be an instance of FieldDoc for sort");
    }

    public Optional<List<BsonValue>> sortValues(ScoreDoc scoreDoc) {
      if (scoreDoc instanceof FieldDoc fieldDoc) {
        var sortValues = Arrays.stream(fieldDoc.fields).map(this::sortKeyToBson).toList();
        return Optional.of(sortValues);
      }
      throw new IllegalStateException("ScoreDoc must be an instance of FieldDoc for sort");
    }

    /**
     * Mongos is able to merge sort results based on Bson ordering. Lucene's {@link FieldDoc}
     * returns an Object[]. The exact types of the elements depend on the comparator provided to
     * lucene.
     */
    private BsonValue sortKeyToBson(Object sortKey) {
      if (sortKey instanceof BsonValue) {
        return (BsonValue) sortKey;
      } else if (sortKey instanceof Number) {
        // Sort by score returns a Float
        return new BsonDouble(((Number) sortKey).doubleValue());
      } else {
        throw new IllegalArgumentException(
            "Sort key '" + sortKey + "' is neither a BsonValue nor Number");
      }
    }
  }

  final class NoopSortValuesProvider implements SortValuesProvider {

    @Override
    public Optional<SortField[]> sortFields(TopDocs topDocs) {
      return Optional.empty();
    }

    @Override
    public Optional<List<BsonValue>> sortValues(ScoreDoc scoreDoc) {
      return Optional.empty();
    }
  }
}
