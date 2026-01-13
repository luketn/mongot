package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * A custom sort that mimics MQL sort semantics on heterogeneous fields. See <a
 * href="https://www.mongodb.com/docs/v5.3/reference/bson-type-comparison-order/">BSON Sort
 * Order</a>
 */
public class MqlMixedSort extends SortField {
  private final SortOptions options;
  private final FieldPath field;
  Optional<FieldPath> embeddedRoot;

  public MqlMixedSort(MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    super(sortField.field().toString(), Type.CUSTOM, sortField.options().order().isReverse());
    this.options = sortField.options();
    this.field = sortField.field();
    this.embeddedRoot = embeddedRoot;
  }

  @Override
  public FieldComparator<BsonValue> getComparator(int numHits, Pruning unused) {
    MixedFieldComparator doubles =
        new MixedFieldComparator(
            TypeField.NUMBER_DOUBLE_V2, BsonType.DOUBLE, this.field, this.embeddedRoot);
    MixedFieldComparator longs =
        new MixedFieldComparator(
            TypeField.NUMBER_INT64_V2, BsonType.INT64, this.field, this.embeddedRoot);
    MixedFieldComparator dates =
        new MixedFieldComparator(
            TypeField.DATE_V2, BsonType.DATE_TIME, this.field, this.embeddedRoot);
    MixedFieldComparator tokens =
        new MixedFieldComparator(TypeField.TOKEN, BsonType.STRING, this.field, this.embeddedRoot);
    MixedFieldComparator booleans =
        new MixedFieldComparator(
            TypeField.BOOLEAN, BsonType.BOOLEAN, this.field, this.embeddedRoot);
    MixedFieldComparator uuids =
        new MixedFieldComparator(TypeField.UUID, BsonType.BINARY, this.field, this.embeddedRoot);
    MixedFieldComparator nulls =
        new MixedFieldComparator(TypeField.NULL, BsonType.NULL, this.field, this.embeddedRoot);
    MixedFieldComparator objectIds =
        new MixedFieldComparator(
            TypeField.OBJECT_ID, BsonType.OBJECT_ID, this.field, this.embeddedRoot);

    MixedFieldComparator[] comp = {
      doubles, longs, dates, tokens, uuids, nulls, objectIds, booleans
    };
    return CompositeComparator.create(comp, (UserFieldSortOptions) this.options, numHits);
  }
}
