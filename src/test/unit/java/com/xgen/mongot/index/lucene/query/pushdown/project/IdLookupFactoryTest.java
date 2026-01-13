package com.xgen.mongot.index.lucene.query.pushdown.project;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.MetaField;
import com.xgen.mongot.util.SingleLinkedList;
import com.xgen.mongot.util.bson.ByteUtils;
import com.xgen.testing.LuceneIndexRule;
import java.io.IOException;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ScoreDoc;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class IdLookupFactoryTest {
  @ClassRule public static final LuceneIndexRule indexer = new LuceneIndexRule();

  @BeforeClass
  public static void before() {
    indexer.add(
        SingleLinkedList.<IndexableField>empty()
            .prepend(
                new StoredField(
                    MetaField.ID.getLuceneFieldName(),
                    ByteUtils.toBytesRef(RawBsonDocument.parse("{_id: 0}"))))
            .prepend(
                new StoredField(
                    FieldName.StaticField.STORED_SOURCE.getLuceneFieldName(),
                    ByteUtils.toBytesRef(RawBsonDocument.parse("{a: 0}")))));
    indexer.add(
        SingleLinkedList.<IndexableField>empty()
            .prepend(
                new StoredField(
                    FieldName.StaticField.STORED_SOURCE.getLuceneFieldName(),
                    ByteUtils.toBytesRef(RawBsonDocument.parse("{a: 1}"))))
            .prepend(
                new StoredField(
                    MetaField.ID.getLuceneFieldName(),
                    ByteUtils.toBytesRef(RawBsonDocument.parse("{_id: 1}")))));
  }

  @Test
  public void projectSequential() throws IOException {
    IdLookupFactory factory = new IdLookupFactory(indexer.getIndexReader());
    ProjectStage projectStage =
        factory.create(new ScoreDoc[] {new ScoreDoc(0, 1f), new ScoreDoc(1, 1f)});

    assertThat(projectStage.getId(0)).hasValue(new BsonInt32(0));
    assertThat(projectStage.project(0)).isEmpty();

    assertThat(projectStage.project(1)).isEmpty();
    assertThat(projectStage.getId(1)).hasValue(new BsonInt32(1));
  }

  @Test
  public void projectOutOfOrder() throws IOException {
    IdLookupFactory factory = new IdLookupFactory(indexer.getIndexReader());
    ProjectStage projectStage =
        factory.create(new ScoreDoc[] {new ScoreDoc(1, 1f), new ScoreDoc(0, 1f)});

    assertThat(projectStage.project(1)).isEmpty();
    assertThat(projectStage.getId(1)).hasValue(new BsonInt32(1));

    assertThat(projectStage.getId(0)).hasValue(new BsonInt32(0));
    assertThat(projectStage.project(0)).isEmpty();
  }
}
