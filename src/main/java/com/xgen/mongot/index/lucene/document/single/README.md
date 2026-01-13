# Lucene Indexing - Single Document Field and Value Handlers

The `single` package contains:

- `DocumentHandler` and `FieldValueHandler`s related to creating a single Lucene Document
    - [RootDocumentBuilder](RootDocumentBuilder.java)
        - [LuceneDocumentBuilder](LuceneDocumentBuilder.java)
    - [LuceneDocumentHandler](LuceneDocumentHandler.java)
    - [LuceneFieldValueHandler](LuceneFieldValueHandler.java)
- Classes that create Lucene indexable fields and insert them into Lucene documents
    - [IndexableFieldFactory](IndexableFieldFactory.java)
- Containers for related information
    - [DocumentWrapper](DocumentWrapper.java)

## DocumentWrapper

A [DocumentWrapper](DocumentWrapper.java) is a container that pairs a Lucene `Document` and an
optional `FieldPath` representing the embedded root path that the Lucene document is indexed at. The
embedded root path is used to create Lucene field names
via [FieldName](/src/main/java/com/xgen/mongot/index/lucene/field/FieldName.java).

When making choices about if a value type should be deserialized, it is not important if a field is
embedded or not - the configuring `FieldDefinition` and if the field is multi-valued (part of an
array or an array of documents) is the only information needed to make that determination.

Grouping the Lucene `Document` with the embedded root path of that document lets `DocumentHandler`s
and `FieldValueHandler`s operate without knowledge of whether the Lucene document they are building
is part of an embedded document or not, and gives them an easy way to delegate that information to
places that create indexable fields and insert them into documents.

## DocumentHandlers and FieldValueHandlers

[RootDocumentBuilder](RootDocumentBuilder.java) is a good entrypoint to understand logic in this
package - it is responsible for creating a composite `DocumentHandler` which wraps `DocumentHandler`
s to build stored-source-bson-document and lucene-document index components side by side.

### LuceneDocumentBuilder

[LuceneDocumentBuilder](LuceneDocumentBuilder.java) is a `DocumentHandler` responsible for
creating `FieldValueHandler`s for fields at the root level of an indexed document.

#### Root Non-Embedded Documents

When building the root, "non-embedded" Lucene document, `LuceneDocumentBuilder` is configured with:

- The `DocumentFieldDefinition` (e.g. `mappings`) for a particular index.
- A [DocumentWrapper](#documentwrapper) that contains a Lucene document
    - **with a `$meta/_id` field**
        - all documents contain a field with the encoded _id of the MongoDB source document they
          originated from
    - without a `$meta/embeddedRoot` field
        - this document is not the root document of an embedded document block
    - without a `$meta/embeddedPath` field
        - this document is not a child document that is part of an embedded document block

#### Root Embedded Documents

When building the root, embedded Lucene document, `LuceneDocumentBuilder` is configured with:

- The `DocumentFieldDefinition` (e.g. `mappings`) for a particular index.
- A [DocumentWrapper](#documentwrapper) that contains a Lucene document
    - **with a `$meta/_id` field**
        - all documents contain a field with the encoded _id of the MongoDB source document they
          originated from
    - **with a `$meta/embeddedRoot` field**
        - this document **is the root document** of an embedded document block
    - without a `$meta/embeddedPath` field
        - this document is not a child document that is part of an embedded document block

#### Non-Root Embedded Documents

When building an embedded Lucene document, `LuceneDocumentBuilder` is configured with:

- The `EmbeddedDocumentsFieldDefinition` configuring the root of that embedded document.
- A [DocumentWrapper](#documentwrapper) that contains a Lucene document
    - **with a `$meta/_id` field**
        - all documents contain a field with the encoded _id of the MongoDB source document they
          originated from
    - without a `$meta/embeddedRoot` field
        - this document is not the root document of an embedded document block
    - **with a `$meta/embeddedPath` field**
        - this document **is a child document** that is part of an embedded document block

#### Creating FieldValueHandlers

After instantiation, `LuceneDocumentBuilder` is ignorant of its "embedded"-ness. It may create
a [LuceneFieldValueHandler](LuceneFieldValueHandler.java), if a field is configured to be indexed
either with a static field definition or dynamically.

### LuceneFieldValueHandler

[LuceneFieldValueHandler](LuceneFieldValueHandler.java) is responsible for indexing values given a
static or dynamically defined field definition, and creates
a [LuceneDocumentHandler](LuceneDocumentHandler.java) if a document field is configured to be
indexed.

`LuceneFieldValueHandler` contains most of the logic that translates field definition configuration
into indexable fields in an indexed Lucene document.

### LuceneDocumentHandler

[LuceneDocumentHandler](LuceneDocumentHandler.java) is similar to a `LuceneDocumentBuilder`, but:

- It is not at the root of a document (it has a `FieldPath`).
- It does not know how to build a Lucene document.

A `LuceneDocumentHandler` may create a [LuceneFieldValueHandler](LuceneFieldValueHandler.java) to
handle value at particular fields, if a field is configured to be indexed.

## `IndexableFieldFactory`

Package also contains stateless class [IndexableFieldFactory](IndexableFieldFactory.java), which
inserts indexable fields into Lucene documents given values and parameters for indexing.

## Recommended Reading

- [BsonDocumentProcessor Package](/src/main/java/com/xgen/mongot/index/ingestion/README.md)
    - [DocumentHandler and FieldValueHandler Package](/src/main/java/com/xgen/mongot/index/ingestion/handlers/README.md)
- [Document Package](/src/main/java/com/xgen/mongot/index/lucene/document/README.md)
    - [*(this
      readme)* Single Document Field and Value Handlers Package](/src/main/java/com/xgen/mongot/index/lucene/document/single/README.md)
