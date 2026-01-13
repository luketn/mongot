package com.xgen.mongot.index.lucene.query.pushdown.project;

import com.google.errorprone.annotations.ThreadSafe;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.bson.RawBsonDocument;

/**
 * This class is responsible for instantiating {@link ProjectStage}s once per batch of hits. Unlike
 * ProjectStage, implementations of this class are stateless and threadsafe. This class should be
 * instantiated once per query.
 */
@ThreadSafe
public interface ProjectFactory {

  /**
   * Creates a {@link ProjectStage} for a given batch of search hits.
   *
   * <p>Depending on the implementation, projections and _ids may need to be eagerly computed for
   * the entire batch. Therefore, efficient usage relies on accurate batchSize estimation.
   */
  ProjectStage create(ScoreDoc[] resultSet) throws IOException;

  /** Returns a factory that can build a projection strategy per batch of search hits. */
  static ProjectFactory build(ProjectSpec spec, IndexReader indexReader) {
    if (spec.returnStoredSource) {
      ProjectionSource<RawBsonDocument> source = StoredSourceStrategy::new;
      return new UnorderedProjectFactory(indexReader, source);
    }

    return new IdLookupFactory(indexReader);
  }
}
