package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;

class SingleQueryContext {

  enum QueryAssociation {
    WITH_OPERATOR,
    NONE
  }

  private final IndexReader indexReader;
  private final QueryAssociation queryAssociation;
  private final Optional<FieldPath> operatorPath;
  private final Optional<FieldPath> embeddedRoot;

  SingleQueryContext(
      IndexReader indexReader,
      QueryAssociation queryAssociation,
      Optional<FieldPath> operatorPath,
      Optional<FieldPath> embeddedRoot) {
    this.indexReader = indexReader;
    this.queryAssociation = queryAssociation;
    this.operatorPath = operatorPath;
    this.embeddedRoot = embeddedRoot;
  }

  static SingleQueryContext createQueryRoot(IndexReader indexReader) {
    return new SingleQueryContext(
        indexReader, QueryAssociation.NONE, Optional.empty(), Optional.empty());
  }

  static SingleQueryContext createQueryRootWithReturnScope(
      IndexReader indexReader, Optional<ReturnScope> returnScope) {
    return new SingleQueryContext(
        indexReader, QueryAssociation.NONE, Optional.empty(), returnScope.map(ReturnScope::path));
  }

  static SingleQueryContext createExplainRoot(IndexReader indexReader) {
    return new SingleQueryContext(
        indexReader, QueryAssociation.WITH_OPERATOR, Optional.empty(), Optional.empty());
  }

  static SingleQueryContext createExplainRootWithReturnScope(
      IndexReader indexReader, Optional<ReturnScope> returnScope) {
    return new SingleQueryContext(
        indexReader,
        QueryAssociation.WITH_OPERATOR,
        Optional.empty(),
        returnScope.map(ReturnScope::path));
  }

  SingleQueryContext withEmbeddedRoot(FieldPath embeddedRoot) {
    return new SingleQueryContext(
        this.indexReader, this.queryAssociation, this.operatorPath, Optional.of(embeddedRoot));
  }

  SingleQueryContext withOperatorPath(Optional<FieldPath> operatorPath) {
    return new SingleQueryContext(
        this.indexReader, this.queryAssociation, operatorPath, this.embeddedRoot);
  }

  IndexReader getIndexReader() {
    return this.indexReader;
  }

  QueryAssociation getQueryAssociation() {
    return this.queryAssociation;
  }

  Optional<FieldPath> getOperatorPath() {
    return this.operatorPath;
  }

  Optional<FieldPath> getEmbeddedRoot() {
    return this.embeddedRoot;
  }
}
