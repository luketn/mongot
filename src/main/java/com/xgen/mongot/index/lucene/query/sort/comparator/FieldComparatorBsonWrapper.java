package com.xgen.mongot.index.lucene.query.sort.comparator;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

public class FieldComparatorBsonWrapper<T> extends FieldComparator<BsonValue> {

  final FieldComparator<T> wrappedComparator;
  final BsonConverter<T> converter;

  /**
   * The BsonValue we should return to represent a document with a missing value. One of {BsonNull,
   * BsonMinKey, BsonMaxKey} depending on value of the `noData` sort parameter.
   */
  final BsonValue missingValueSortKey;

  public FieldComparatorBsonWrapper(
      FieldComparator<T> wrappedComparator,
      BsonConverter<T> converter,
      BsonValue missingValueSortKey) {
    this.wrappedComparator = wrappedComparator;
    this.converter = converter;
    this.missingValueSortKey = missingValueSortKey;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return this.wrappedComparator.compare(slot1, slot2);
  }

  @Override
  public void setTopValue(BsonValue value) {
    this.wrappedComparator.setTopValue(
        this.converter.decodeFromBson(value, this.missingValueSortKey));
  }

  @Override
  public int compareValues(BsonValue first, BsonValue second) {
    return this.wrappedComparator.compareValues(
        this.converter.decodeFromBson(first, this.missingValueSortKey),
        this.converter.decodeFromBson(second, this.missingValueSortKey));
  }

  @Override
  public BsonValue value(int slot) {
    @Nullable T value = this.wrappedComparator.value(slot);
    return this.converter.encodeToBson(value, this.missingValueSortKey);
  }

  @Override
  public void setSingleSort() {
    this.wrappedComparator.setSingleSort();
  }

  @Override
  public void disableSkipping() {
    this.wrappedComparator.disableSkipping();
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return this.wrappedComparator.getLeafComparator(context);
  }
}
