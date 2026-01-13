# Index Ingestion

`com.xgen.mongot.index.ingestion`

The `ingestion` package contains `BsonDocumentProcessor`, responsible for traversing
a `BsonDocument` and deserializing `BsonValue`s.

`BsonDocumentProcessor` is entirely stateless. External callers use `BsonDocumentProcessor` by
calling the `process` method, which accepts a `RawBsonDocument`
and [`DocumentHandler`](handlers/README.md) as arguments.

There are two main interfaces that `BsonDocumentProcessor` uses to build an index
document, [DocumentHandler](handlers/DocumentHandler.java)
and [FieldValueHandler](handlers/FieldValueHandler.java). Classes in the `handlers` package are
responsible for determining whether a value type should be deserialized and handling a deserialized
value if an index is so configured, are described more in the [handlers](handlers/README.md)
package.

For the purposes of this package, `FieldValueHandler` knows if and how to index values,
and `DocumentHandler` knows how to produce a `FieldValueHandler` for a particular field.

In the process of traversing a source document, `BsonDocumentProcessor` uses `DocumentHandler`s to
represent the state of a source document after a bson document is opened, before a field name is
read.

```java
public static void process(RawBsonDocument bsonDocument, DocumentHandler documentHandler) {
  try (BsonBinaryReader bsonReader =
      new BsonBinaryReader(new ByteBufferBsonInput(bsonDocument.getByteBuffer()))) {
    handleDocumentField(bsonReader, documentHandler);
  }
}
```

When reading a field name, `BsonDocumentProcessor` uses `DocumentHandler` to create
a `FieldValueHandler` for that particular field.

```java
static void handleDocumentField(BsonBinaryReader bsonReader, DocumentHandler documentHandler) {
  bsonReader.readStartDocument();

  while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
    documentHandler
        .valueHandler(bsonReader.readName())
        .ifPresentOrElse(
            handler -> {
              handler.markFieldNameExists();
              handleField(bsonReader, handler);
            },
            bsonReader::skipValue);
  }

  bsonReader.readEndDocument();
}
```

`BsonDocumentProcessor` provides a lazy supplier to field value handlers, only deserializing values
if the lazy supplier is exercised by a field value handler.

```java
case STRING:
  try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readString)) {
    fieldValueHandler.handleString(supplier);
  }
  return;
```

Finally, a `FieldValueHandler` can be used to create a new `DocumentHandler` when reading a sub
document.

```java
case DOCUMENT:
  fieldValueHandler
      .subDocumentHandler()
      .ifPresentOrElse(
          handler -> handleDocumentField(bsonReader, handler), bsonReader::skipValue);
```

## Adding a new Field Definition

### For BsonType(s) that we already deserialize, add logic in `LuceneFieldValueHandler#handleX`

_e.g. add "autocomplete2" index field type, which describes a new way to index String-type values_

Add logic in the `handleX` method for the data type(s) this field is interested in.

For `autocomplete2`, this means a change under `handleString`.

```java
@Override
public void handleString(Supplier<String> supplier) {
  this.fieldDefinition
      .getStringFieldDefinition()
      .ifPresent(
          stringField ->
              doHandleString(
                  this.documentWrapper,
                  stringField,
                  this.path,
                  supplier.get()));

  // ...

  this.fieldDefinition
      .getAutocomplete2FieldDefinition()
      .ifPresent(
          autocomplete2Field ->
              doHandleString(
                  this.documentWrapper,
                  autocomplete2Field,
                  this.path,
                  supplier.get()));
}
```

In the case `autocomplete2` was interested in more than just string types, it could add logic under
other `handleX` methods to index those types as well.

### For a BsonType that we don't already deserialize

In this example, consider adding support for indexing `DECIMAL128`-type `BsonValue`s as `BigDecimal`
values.

#### 1. Change `BsonDocumentProcessor` to handle that new BsonType

Add a case for `DECIMAL128` in `handleField` that deserializes a `BsonDecimal128` to `BigDecimal`,
and passes that `BigDecimal` to a `FieldValueHandler`.

```java
case DECIMAL128:
  try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readDecimal128)) {
    fieldValueHandler.handleDecimal128(supplier);
  }
  return;
```

#### 2. Add a `handleBigDecimal` method to `FieldValueHandler`s:

There are a handful, but changes are all relatively small. The below are _all the changes_ that
would be needed to add `BigDecimal` support.

##### `FieldValueHandler`

```java
void handleDecimal128(Supplier<Decimal128> supplier);
```

##### `LuceneFieldValueHandler`

```java
@Override
public void handleBigDecimal(Supplier<Decimal128> supplier) {
  if (this.fieldDefinition.getBigDecimalDefinition().isPresent()) {
    IndexableFieldFactory.addObjectIdField(
        this.documentBuilder.luceneDocument,
        this.path,
        supplier.get());
  }
}
```

##### `StoredFieldValueHandler`

```java
@Override
public void handleBigDecimal(Supplier<BigDecimal> supplier) {
  handleValueIfStored(supplier, decimal128 -> new BsonDecimal128(new Decimal128(decimal128)));
}
```

##### `CompositeFieldValueHandler`

```java
@Override
public void handleBigDecimal(Supplier<BigDecimal> supplier) {
  this.first.handleBigDecimal(supplier);
  this.second.handleBigDecimal(supplier);
}
```

##### `EmbeddedFieldValueHandler`

```java
@Override
public void handleBigDecimal(Supplier<BigDecimal> supplier) {
  this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleBigDecimal(supplier));
}
```

#### 3. Add a `addBigDecimal` method to `IndexableFieldFactory`:

Add a method `addBigDecimal` to `IndexableFieldFactory` that actually inserts an indexed value into
a Lucene document.

```java
static void addBigDecimalField(
    AbstractDocumentWrapper document, FieldPath path, BigDecimal value) {
  String luceneFieldName =
      FieldName.TypeField.BIG_DECIMAL.getLuceneFieldName(path, document.getEmbeddedRoot());
  IndexableField indexableField = null; // instantiate indexable field for big decimal
  document.put(indexableField);
}
```

## Recommended Reading

- [*(this readme)* BsonDocumentProcessor Package](/src/main/java/com/xgen/mongot/index/ingestion/README.md)
    - [DocumentHandler and FieldValueHandler Package](/src/main/java/com/xgen/mongot/index/ingestion/handlers/README.md)
- [Document Package](/src/main/java/com/xgen/mongot/index/lucene/document/README.md)
    - [Single Document and Field Value Handlers Package](/src/main/java/com/xgen/mongot/index/lucene/document/single/README.md)
