package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.bson.BsonDouble;
import org.junit.Test;

public class KnnQueryFactoryTest {

  private static final String PATH_A = "a";
  private static final String PATH_B = "b";

  private static final DocumentFieldDefinition MAPPED_TO_KNN =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field(
              PATH_A,
              FieldDefinitionBuilder.builder()
                  .knnVector(
                      KnnVectorFieldDefinitionBuilder.builder()
                          .similarity(VectorSimilarity.COSINE)
                          .dimensions(2)
                          .build())
                  .build())
          .field(
              PATH_B,
              FieldDefinitionBuilder.builder()
                  .knnVector(
                      KnnVectorFieldDefinitionBuilder.builder()
                          .similarity(VectorSimilarity.COSINE)
                          .dimensions(2)
                          .build())
                  .build())
          .build();

  @Test
  public void testBasicKnnBeta() throws Exception {
    var definition =
        OperatorBuilder.knnBeta()
            .path(PATH_A)
            .vector(List.of(new BsonDouble(0.1), new BsonDouble(0.2)))
            .k(10)
            .build();

    var expected = new KnnFloatVectorQuery("$type:knnVector/a", new float[] {0.1f, 0.2f}, 10);

    LuceneSearchTranslation.mapped(MAPPED_TO_KNN).assertTranslatedTo(definition, expected);
  }

  @Test
  public void testBasicKnnBetaWithMultiplePaths() throws Exception {
    var definition =
        OperatorBuilder.knnBeta()
            .path(PATH_A)
            .path(PATH_B)
            .vector(List.of(new BsonDouble(0.1), new BsonDouble(0.2)))
            .k(10)
            .build();

    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    new KnnFloatVectorQuery("$type:knnVector/a", new float[] {0.1f, 0.2f}, 10),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    new KnnFloatVectorQuery("$type:knnVector/b", new float[] {0.1f, 0.2f}, 10),
                    BooleanClause.Occur.SHOULD))
            .build();

    LuceneSearchTranslation.mapped(MAPPED_TO_KNN).assertTranslatedTo(definition, expected);
  }

  @Test
  public void testKnnBetaWithFilter() throws Exception {
    var filter = OperatorBuilder.text().query("query").path("path").build();

    var definition =
        OperatorBuilder.knnBeta()
            .path(PATH_A)
            .vector(List.of(new BsonDouble(0.1), new BsonDouble(0.2)))
            .filter(filter)
            .k(10)
            .build();

    var expected =
        new KnnFloatVectorQuery(
            "$type:knnVector/a",
            new float[] {0.1f, 0.2f},
            10,
            new TermQuery(new Term("$type:string/path", "query")));

    LuceneSearchTranslation.mapped(MAPPED_TO_KNN).assertTranslatedTo(definition, expected);
  }

  @Test
  public void testRuntimeCheckOnNonIndexedKnnField() {
    var definition =
        OperatorBuilder.knnBeta()
            .path(PATH_A)
            .vector(List.of(new BsonDouble(0.1), new BsonDouble(0.2)))
            .k(10)
            .build();

    TestUtils.assertThrows(
        "is not indexed as knnVector",
        InvalidQueryException.class,
        () ->
            LuceneSearchTranslation.get()
                .assertTranslatedTo(
                    definition,
                    new KnnFloatVectorQuery("$type:knnVector/a", new float[] {0.1f, 0.2f}, 10)));
  }

  @Test
  public void testRuntimeCheckOnIncorrectNumberOfDimensions() {
    var definition =
        OperatorBuilder.knnBeta()
            .path(PATH_A)
            .vector(List.of(new BsonDouble(0.1), new BsonDouble(0.2), new BsonDouble(0.3)))
            .k(10)
            .build();

    TestUtils.assertThrows(
        "knnVector field is indexed with 2 dimensions but queried with 3",
        InvalidQueryException.class,
        () ->
            LuceneSearchTranslation.mapped(MAPPED_TO_KNN)
                .assertTranslatedTo(
                    definition,
                    new KnnFloatVectorQuery("$type:knnVector/a", new float[] {0.1f, 0.2f}, 10)));
  }
}
