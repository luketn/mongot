package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.sort.comparator.BsonConverter;
import com.xgen.mongot.index.lucene.query.sort.comparator.FieldComparatorBsonWrapper;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.comparators.TermOrdValComparator;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBoolean;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.NotNull;

public class MqlSortedSetSortField extends SortedSetSortField {
  private static final BytesRef TRUE_BYTES = new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE);

  private final BsonConverter<BytesRef> converter;
  private final boolean enablePruning;
  private final BsonValue missingValueSortValue;

  private MqlSortedSetSortField(
      String fieldName,
      SortOptions options,
      BsonConverter<BytesRef> converter,
      boolean enablePruning) {
    super(fieldName, options.isReverse(), options.selector().sortedSetSelector);

    setMissingValue(
        ((UserFieldSortOptions) options).nullEmptySortPosition() == NullEmptySortPosition.HIGHEST
            ? STRING_LAST
            : STRING_FIRST);

    this.missingValueSortValue =
        ((UserFieldSortOptions) options).nullEmptySortPosition().getNullMissingSortValue();

    this.converter = converter;
    this.enablePruning = enablePruning;
  }

  @Override
  public FieldComparator<?> getComparator(int numHits, Pruning lucenePruning) {
    // Consider both mongot & lucene pruning configuration
    Pruning configurePruning = this.enablePruning ? lucenePruning : Pruning.NONE;

    TermOrdValComparator termOrdValComparator =
        new TermOrdValComparator(
            numHits,
            getField(),
            this.missingValue == STRING_LAST,
            super.getReverse(),
            configurePruning) {

          @Override
          protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
              throws IOException {
            return SortedSetSelector.wrap(
                DocValues.getSortedSet(context.reader(), field),
                MqlSortedSetSortField.super.getSelector());
          }
        };

    return new FieldComparatorBsonWrapper<>(
        termOrdValComparator, this.converter, MqlSortedSetSortField.this.missingValueSortValue);
  }

  public static MqlSortedSetSortField booleanSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(FieldValue.fromBoolean(value.asBoolean().getValue()));
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonBoolean.valueOf(value.bytesEquals(TRUE_BYTES));
          }
        };

    var fieldName = resolveFieldName(FieldName.TypeField.BOOLEAN, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true);
  }

  public static MqlSortedSetSortField uuidSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asBinary().asUuid().toString());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonUtils.UUID_CONVERTER.apply(value);
          }
        };

    var fieldName = resolveFieldName(FieldName.TypeField.UUID, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true);
  }

  public static MqlSortedSetSortField objectIdSort(
      MongotSortField sortField, Optional<FieldPath> embeddedRoot) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asObjectId().getValue().toByteArray());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return BsonUtils.OBJECT_ID_CONVERTER.apply(value);
          }
        };

    var fieldName =
        resolveFieldName(FieldName.TypeField.OBJECT_ID, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, true);
  }

  public static MqlSortedSetSortField stringSort(
      FieldName.TypeField fieldType,
      MongotSortField sortField,
      boolean enablePruning,
      Optional<FieldPath> embeddedRoot) {
    var converter =
        new BsonConverter<BytesRef>(null) {
          @Override
          public BytesRef decodeNonMissingValue(BsonValue value) {
            return new BytesRef(value.asString().getValue());
          }

          @Override
          public BsonValue encodeNonMissingValue(BytesRef value) {
            return new BsonString(value.utf8ToString());
          }
        };

    var fieldName = resolveFieldName(fieldType, sortField.field(), embeddedRoot);
    return new MqlSortedSetSortField(fieldName, sortField.options(), converter, enablePruning);
  }

  private static @NotNull String resolveFieldName(
      FieldName.TypeField fieldType, FieldPath fieldPath, Optional<FieldPath> embeddedRoot) {
    return fieldType.getLuceneFieldName(fieldPath, embeddedRoot);
  }
}
