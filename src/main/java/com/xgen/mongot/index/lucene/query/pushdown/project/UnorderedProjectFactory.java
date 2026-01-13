package com.xgen.mongot.index.lucene.query.pushdown.project;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * This {@link ProjectFactory} is optimized for {@link ProjectStrategy}s that support random access
 * and do not require _id (e.g. returnStoredSource).
 */
class UnorderedProjectFactory implements ProjectFactory {

  private final IndexReader reader;
  private final ProjectionSource<RawBsonDocument> strategy;

  UnorderedProjectFactory(IndexReader reader, ProjectionSource<RawBsonDocument> strategy) {
    this.reader = reader;
    this.strategy = strategy;
  }

  @Override
  public ProjectStage create(ScoreDoc[] resultSet) throws IOException {
    ProjectStrategy<RawBsonDocument> projectStrategy = this.strategy.create(this.reader);
    return new UnorderedProjectStage(projectStrategy);
  }

  private static class UnorderedProjectStage implements ProjectStage {
    private final ProjectStrategy<RawBsonDocument> strategy;

    UnorderedProjectStage(ProjectStrategy<RawBsonDocument> strategy) {
      this.strategy = strategy;
    }

    @Override
    public Optional<BsonDocument> project(int docId) throws IOException {
      return Optional.of(this.strategy.project(docId));
    }

    @Override
    public Optional<BsonValue> getId(int docId) {
      return Optional.empty();
    }
  }
}
