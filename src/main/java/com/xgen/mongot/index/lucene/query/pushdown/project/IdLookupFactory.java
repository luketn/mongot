package com.xgen.mongot.index.lucene.query.pushdown.project;

import com.xgen.mongot.index.lucene.field.FieldName.MetaField;
import com.xgen.mongot.index.lucene.query.util.MetaIdRetriever;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * This {@link ProjectFactory} is the default behavior when the user specifies neither
 * `returnStoredSource` nor a pushdown-compatible $project stage. <br>
 *
 * <p>This class looks up {@link MetaField#ID} which will populate {@code SearchResult.id}. This is
 * then used in the idLookUp stage on the server.
 */
class IdLookupFactory implements ProjectFactory {

  private final IndexReader reader;

  IdLookupFactory(IndexReader reader) {
    this.reader = reader;
  }

  @Override
  public ProjectStage create(ScoreDoc[] unused) throws IOException {
    // Underlying implementation uses storedFields, which are not thread safe, so must create a
    // wrapper per-batch
    return new IdLookupStage(MetaIdRetriever.create(this.reader));
  }

  private static class IdLookupStage implements ProjectStage {

    private final MetaIdRetriever metaIdRetriever;

    private IdLookupStage(MetaIdRetriever metaIdRetriever) {
      this.metaIdRetriever = metaIdRetriever;
    }

    @Override
    public Optional<BsonValue> getId(int docId) throws IOException {
      return Optional.of(this.metaIdRetriever.getRootMetaId(docId));
    }

    @Override
    public Optional<BsonDocument> project(int docId) {
      return Optional.empty();
    }
  }
}
