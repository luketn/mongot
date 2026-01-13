package com.xgen.mongot.index;

/**
 * Container for different document counts. numDocs measures the amount of Lucene documents indexed.
 * In most cases, this also represents the amount of MongoDB documents indexed. However, in embedded
 * indexes, we need a separate count, as the number of lucene docs may be higher than MongoDB docs.
 */
public class DocCounts {

  public final long numDocs;
  public final long numLuceneMaxDocs;
  public final int maxLuceneMaxDocs;
  public final long numMongoDbDocs;

  public DocCounts(long numDocs, long numLuceneMaxDocs, int maxLuceneMaxDocs, long numMongoDbDocs) {
    this.numDocs = numDocs;
    this.numLuceneMaxDocs = numLuceneMaxDocs;
    this.maxLuceneMaxDocs = maxLuceneMaxDocs;
    this.numMongoDbDocs = numMongoDbDocs;
  }
}
