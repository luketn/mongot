package com.xgen.proto;

import java.util.Set;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/**
 * Wraps a BsonReader to filter out the passed fields, but only in the context of the current
 * document -- any sub-documents are processed as-is.
 */
public class DiscriminatedUnionFieldFilteringBsonReader implements BsonReader {
  private final BsonReader reader;
  private final Set<String> filteredFields;
  private int documentLevel = 0;

  /**
   * Create a new BsonReader that filters the named fields in the context of the current document.
   * These fields will not be filtered in any sub-documents or arrays.
   *
   * @param reader reader to apply the filter to.
   * @param filteredFields names of all fields to remove.
   * @return a BsonReader that will not yield any of filteredFields from the current document.
   */
  public static BsonReader create(BsonReader reader, Set<String> filteredFields) {
    if (filteredFields.isEmpty()) {
      return reader;
    } else {
      return new DiscriminatedUnionFieldFilteringBsonReader(reader, filteredFields);
    }
  }

  private DiscriminatedUnionFieldFilteringBsonReader(
      BsonReader reader, Set<String> filteredFields) {
    this.reader = reader;
    this.filteredFields = filteredFields;
  }

  // This method is responsible for implementing the field filtering.
  //
  // Filtering is only applied to the first document level seen; embedded documents are ignored.
  // To filter we read the name at the same time we read the type and skip value if it is a filtered
  // name. readName() calls are aliased to getCurrentName() when field filtering.
  @Override
  public BsonType readBsonType() {
    // Assuming we start before a document, only filter when we are in the context of the first
    // document seen in this reader and not any sub-documents.
    if (this.documentLevel != 1) {
      return this.reader.readBsonType();
    }

    while (this.reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      if (this.filteredFields.contains(this.reader.readName())) {
        this.reader.skipValue();
      } else {
        return this.reader.getCurrentBsonType();
      }
    }

    // NB: getCurrentBsonType() may return junk at END_OF_DOCUMENT.
    return BsonType.END_OF_DOCUMENT;
  }

  @Override
  public String readName() {
    // If we are in documentLevel 1 we are filtering and the name has likely already been read.
    return this.documentLevel == 1 ? this.reader.getCurrentName() : this.reader.readName();
  }

  @Override
  public void readName(String s) {
    this.reader.readName(s);
  }

  // When we enter a sub-document increase documentLevel to disable filtering.
  @Override
  public void readStartDocument() {
    this.documentLevel += 1;
    this.reader.readStartDocument();
  }

  // When we exit a sub-document decrease documentLevel to potentially re-enable filtering.
  @Override
  public void readEndDocument() {
    this.documentLevel -= 1;
    this.reader.readEndDocument();
  }

  // Methods below this point simply delegate to the underlying reader type.

  @Override
  public BsonType getCurrentBsonType() {
    return this.reader.getCurrentBsonType();
  }

  @Override
  public String getCurrentName() {
    return this.reader.getCurrentName();
  }

  @Override
  public BsonReaderMark getMark() {
    return this.reader.getMark();
  }

  @Override
  public void readStartArray() {
    this.reader.readStartArray();
  }

  @Override
  public void readEndArray() {
    this.reader.readEndArray();
  }

  @Override
  public int peekBinarySize() {
    return this.reader.peekBinarySize();
  }

  @Override
  public byte peekBinarySubType() {
    return this.reader.peekBinarySubType();
  }

  @Override
  public BsonBinary readBinaryData() {
    return this.reader.readBinaryData();
  }

  @Override
  public BsonBinary readBinaryData(String s) {
    return this.reader.readBinaryData(s);
  }

  @Override
  public boolean readBoolean() {
    return this.reader.readBoolean();
  }

  @Override
  public boolean readBoolean(String s) {
    return this.reader.readBoolean(s);
  }

  @Override
  public long readDateTime() {
    return this.reader.readDateTime();
  }

  @Override
  public long readDateTime(String s) {
    return this.reader.readDateTime(s);
  }

  @Override
  public BsonDbPointer readDBPointer() {
    return this.reader.readDBPointer();
  }

  @Override
  public BsonDbPointer readDBPointer(String s) {
    return this.reader.readDBPointer(s);
  }

  @Override
  public Decimal128 readDecimal128() {
    return this.reader.readDecimal128();
  }

  @Override
  public Decimal128 readDecimal128(String s) {
    return this.reader.readDecimal128(s);
  }

  @Override
  public double readDouble() {
    return this.reader.readDouble();
  }

  @Override
  public double readDouble(String s) {
    return this.reader.readDouble(s);
  }

  @Override
  public int readInt32() {
    return this.reader.readInt32();
  }

  @Override
  public int readInt32(String s) {
    return this.reader.readInt32(s);
  }

  @Override
  public long readInt64() {
    return this.reader.readInt64();
  }

  @Override
  public long readInt64(String s) {
    return this.reader.readInt64(s);
  }

  @Override
  public String readJavaScript() {
    return this.reader.readJavaScript();
  }

  @Override
  public String readJavaScript(String s) {
    return this.reader.readJavaScript(s);
  }

  @Override
  public String readJavaScriptWithScope() {
    return this.reader.readJavaScriptWithScope();
  }

  @Override
  public String readJavaScriptWithScope(String s) {
    return this.reader.readJavaScriptWithScope(s);
  }

  @Override
  public void readMaxKey() {
    this.reader.readMaxKey();
  }

  @Override
  public void readMaxKey(String s) {
    this.reader.readMaxKey(s);
  }

  @Override
  public void readMinKey() {
    this.reader.readMinKey();
  }

  @Override
  public void readMinKey(String s) {
    this.reader.readMinKey(s);
  }

  @Override
  public void readNull() {
    this.reader.readNull();
  }

  @Override
  public void readNull(String s) {
    this.reader.readNull(s);
  }

  @Override
  public ObjectId readObjectId() {
    return this.reader.readObjectId();
  }

  @Override
  public ObjectId readObjectId(String s) {
    return this.reader.readObjectId(s);
  }

  @Override
  public BsonRegularExpression readRegularExpression() {
    return this.reader.readRegularExpression();
  }

  @Override
  public BsonRegularExpression readRegularExpression(String s) {
    return this.reader.readRegularExpression(s);
  }

  @Override
  public String readString() {
    return this.reader.readString();
  }

  @Override
  public String readString(String s) {
    return this.reader.readString(s);
  }

  @Override
  public String readSymbol() {
    return this.reader.readSymbol();
  }

  @Override
  public String readSymbol(String s) {
    return this.reader.readSymbol(s);
  }

  @Override
  public BsonTimestamp readTimestamp() {
    return this.reader.readTimestamp();
  }

  @Override
  public BsonTimestamp readTimestamp(String s) {
    return this.reader.readTimestamp(s);
  }

  @Override
  public void readUndefined() {
    this.reader.readUndefined();
  }

  @Override
  public void readUndefined(String s) {
    this.reader.readUndefined(s);
  }

  @Override
  public void skipName() {
    this.reader.skipName();
  }

  @Override
  public void skipValue() {
    this.reader.skipValue();
  }

  @Override
  public void close() {
    this.reader.close();
  }
}
