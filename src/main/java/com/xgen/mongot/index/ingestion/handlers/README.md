# Document and Field Value Handlers

[DocumentHandler](DocumentHandler.java) and [FieldValueHandler](FieldValueHandler.java) are main
classes in this package. `DocumentHandler` is responsible for knowing how to
create `FieldValueHandler`s, and `FieldValueHandler` is responsible for knowing if and how to do
most work associated with indexing, along with creating `DocumentHandler`s and `FieldValueHandler`s
for sub documents and arrays.

## DocumentHandler

When document and field value handlers create other document and field value handlers for subfields,
sub documents, and sub arrays, those "child" handlers are instantiated with immutable state. That
state, for [LuceneDocumentHandler](../../lucene/document/single/LuceneDocumentHandler.java),
includes the `DocumentFieldDefinition` or `EmbeddedDocumentsFieldDefinition` of the index at that
particular part of the source document.
For [StoredDocumentHandler](../stored/StoredDocumentHandler.java), that includes things like
the `StoredSourceDefinition` of the index and the absolute path of the source document for that
document handler.

## FieldValueHandler

`FieldValueHandler` implements the brunt of indexing logic, and is responsible for:

- actually indexing a value
- creating `FieldValueHandler`s for sub-arrays
- creating `DocumentHandler`s for sub-documents

When creating "child" handlers, `FieldValueHandler` behaves in a similar way to `DocumentHandler` -
child handlers are instantiated with immutable state, and are created with configuration relevant to
the path of the source document they are responsible for indexing.

## Recommended Reading

- [BsonDocumentProcessor Package](/src/main/java/com/xgen/mongot/index/ingestion/README.md)
    - [*(this
      readme)* DocumentHandler and FieldValueHandler Package](/src/main/java/com/xgen/mongot/index/ingestion/handlers/README.md)
- [Document Package](/src/main/java/com/xgen/mongot/index/lucene/document/README.md)
    - [Single Document Field and Value Handlers Package]((/src/main/java/com/xgen/mongot/index/lucene/document/single/README.md))
