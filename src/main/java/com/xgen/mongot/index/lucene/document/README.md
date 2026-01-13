# Lucene Indexing - Document Package

The `document` package is responsible for building Lucene `Document`s to insert into an index.

[DefaultIndexingPolicy](DefaultIndexingPolicy.java) creates
a [LuceneIndexingPolicy](LuceneIndexingPolicy.java), which is in turn able to
instantiate [DocumentBlockBuilder](builder/DocumentBlockBuilder.java)s given the `byte[]` id of a
given source document.

A `DocumentBlockBuilder` is able to create a `List<Document>` that can be inserted into a Lucene
index. `LuceneIndexWriter` is instantiated with a `LuceneIndexingPolicy` that lets it
create `DocumentBlockBuilder`s for source documents.

A `DocumentBlockBuilder` is also
a [DocumentHandler](/com/xgen/mongot/index/ingestion/handlers/DocumentHandler.java), which can be
passed in to [BsonDocumentProcessor](/com/xgen/mongot/index/ingestion/BsonDocumentProcessor.java)
along with a source `RawBsonDocument`. See
the [DocumentHandler package README](../../ingestion/handlers/README.md) for more information about
how document handlers are used to build indexable Lucene documents/document blocks.

## Recommended reading:

- [BsonDocumentProcessor Package](/src/main/java/com/xgen/mongot/index/ingestion/README.md)
    - [DocumentHandler and FieldValueHandler Package](/src/main/java/com/xgen/mongot/index/ingestion/handlers/README.md)
- [*(this readme)* Document Package](/src/main/java/com/xgen/mongot/index/lucene/document/README.md)
    - [Single Document and Field Value Handlers Package](/src/main/java/com/xgen/mongot/index/lucene/document/single/README.md)
