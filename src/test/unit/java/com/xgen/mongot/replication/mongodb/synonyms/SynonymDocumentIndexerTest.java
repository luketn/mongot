package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_MAPPING_DEFINITION_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.synonym.SynonymDocument;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.testing.mongot.index.synonym.SynonymDocumentBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SynonymDocumentIndexerTest {

  @Test
  public void testValidDocumentEvents() throws Exception {
    Mocks mocks = Mocks.create();
    Collection<RawBsonDocument> documentEvents = validDocuments();
    ArrayList<SynonymDocument> documents = new ArrayList<>();
    for (var doc : documentEvents) {
      documents.add(SynonymDocument.fromBson(doc));
    }

    mocks.documentIndexer.indexDocumentBatch(documentEvents);

    verify(mocks.builder, times(3)).addDocument(argThat(documents::contains));
    verifyNoMoreInteractions(mocks.builder);
  }

  @Test
  public void testInvalidDocumentEvents() throws Exception {
    Mocks mocks = Mocks.create();

    // three valid docs, followed by two invalid, followed by three valid
    Collection<RawBsonDocument> documentEvents =
        Stream.of(validDocuments(), invalidDocuments(), validDocuments())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    try {
      mocks.documentIndexer.indexDocumentBatch(documentEvents);
    } catch (SynonymSyncException e) {
      Assert.assertEquals("should be invalid", SynonymSyncException.Type.INVALID, e.getType());
      Assert.assertEquals(
          "check error message",
          "failed to analyze string in synonym document: \"synonyms\" is required",
          e.getMessage());
      verify(mocks.builder, times(3)).addDocument(any());
      verifyNoMoreInteractions(mocks.builder);
      return;
    }

    Assert.fail("should have thrown exception");
  }

  @Test
  public void testWrapsBuilderException() throws Exception {
    Mocks mocks =
        Mocks.create(
            (ignored) -> {
              throw SynonymMappingException.invalidSynonymDocument(
                  new IOException("analyzed synonym string to empty"));
            },
            Optional.empty());

    try {
      mocks.documentIndexer.indexDocumentBatch(validDocuments());
    } catch (SynonymSyncException e) {
      Assert.assertEquals("should be invalid", SynonymSyncException.Type.INVALID, e.getType());
      Assert.assertEquals(
          "check error message",
          "failed to analyze string in synonym document: analyzed synonym string to empty",
          e.getMessage());
      verify(mocks.builder).addDocument(any());
      verifyNoMoreInteractions(mocks.builder);
      return;
    }

    Assert.fail("should have thrown");
  }

  @Test
  public void testCompleteUpdatesSynonymRegistry() throws Exception {
    Mocks mocks = Mocks.create();
    SynonymMapping buildResult = mock(SynonymMapping.class);
    clearInvocations(mocks.synonymRegistry);

    when(mocks.builder.build()).thenReturn(buildResult);
    doNothing()
        .when(mocks.synonymRegistry)
        .update(eq(MOCK_SYNONYM_MAPPING_DEFINITION_NAME), eq(buildResult));

    mocks.documentIndexer.complete();

    verify(mocks.builder).build();
    verify(mocks.synonymRegistry).update(eq(MOCK_SYNONYM_MAPPING_DEFINITION_NAME), eq(buildResult));
    verifyNoMoreInteractions(mocks.builder, mocks.synonymRegistry);
  }

  @Test
  public void testWrapsBuilderBuildException() throws Exception {
    Mocks mocks = Mocks.create();
    clearInvocations(mocks.synonymRegistry);

    when(mocks.builder.build())
        .thenAnswer(
            ignored -> {
              throw SynonymMappingException.failSynonymMapBuild(
                  new IOException("unexpected exception building synonym map"));
            });

    try {
      mocks.documentIndexer.complete();
    } catch (SynonymSyncException e) {
      Assert.assertEquals("should be transient", SynonymSyncException.Type.TRANSIENT, e.getType());
      Assert.assertEquals("check error message", "failed to build synonym map", e.getMessage());
      verify(mocks.builder).build();
      verifyNoMoreInteractions(mocks.builder, mocks.synonymRegistry);
      return;
    }

    Assert.fail("should have thrown");
  }

  @Test
  public void testInvalidatingExceptionalCompletion() throws Exception {
    Mocks mocks = Mocks.create();

    for (var dropExceptions :
        List.of(
            SynonymSyncException.createFieldExceeded(
                new FieldExceededLimitsException("exceeded field limit")),
            SynonymSyncException.createInvalid(
                SynonymMappingException.invalidSynonymDocument(new Exception())))) {
      clearInvocations(mocks.synonymRegistry);
      doNothing().when(mocks.synonymRegistry).invalidate(any(), any());

      // should drop mapping in registry on these exceptions
      mocks.documentIndexer.completeExceptionally(dropExceptions);
      verify(mocks.synonymRegistry).invalidate(eq(mocks.definition.name()), any());
      verifyNoMoreInteractions(mocks.synonymRegistry);
    }
  }

  @Test
  public void testFailingExceptionalCompletion() throws Exception {
    Mocks mocks = Mocks.create();
    clearInvocations(mocks.synonymRegistry);
    doNothing().when(mocks.synonymRegistry).fail(any(), any());

    // should drop mapping in registry on fail
    mocks.documentIndexer.completeExceptionally(SynonymSyncException.createFailed("failed"));
    verify(mocks.synonymRegistry).fail(eq(mocks.definition.name()), any());
    verifyNoMoreInteractions(mocks.synonymRegistry);
  }

  @Test
  public void testShutdown() throws Exception {
    Mocks mocks = Mocks.create();

    clearInvocations(mocks.synonymRegistry);
    doNothing().when(mocks.synonymRegistry).fail(any(), any());

    // Does not drop mapping for shutdowns
    mocks.documentIndexer.completeExceptionally(SynonymSyncException.createShutDown());
    verifyNoMoreInteractions(mocks.synonymRegistry);
  }

  @Test
  public void testCollectionDropExceptionExceptionalCompletion() throws Exception {
    Mocks mocks = Mocks.create();
    clearInvocations(mocks.synonymRegistry);

    // should clear registry mapping on dropped exception
    doNothing().when(mocks.synonymRegistry).clear(eq(MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION));
    mocks.documentIndexer.completeExceptionally(SynonymSyncException.createDropped());
    verify(mocks.synonymRegistry).clear(eq(MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION));
    verifyNoMoreInteractions(mocks.synonymRegistry);
  }

  @Test
  public void testTransientExceptionExceptionalCompletion() throws Exception {
    Mocks mocks = Mocks.create();
    clearInvocations(mocks.synonymRegistry);

    // shouldn't drop or clear registry on transient exception
    mocks.documentIndexer.completeExceptionally(
        SynonymSyncException.createTransient(new Exception()));
    verifyNoMoreInteractions(mocks.synonymRegistry);
  }

  @Test
  public void testLimitEnforcement() throws Exception {
    Mocks mocks = Mocks.create(4);
    clearInvocations(mocks.synonymRegistry);

    mocks.documentIndexer.indexDocumentBatch(validDocuments());
    verify(mocks.builder, times(3)).addDocument(any());
    clearInvocations(mocks.builder);

    try {
      mocks.documentIndexer.indexDocumentBatch(validDocuments());
    } catch (SynonymSyncException e) {
      Assert.assertEquals(
          "should be exceeded", SynonymSyncException.Type.FIELD_EXCEEDED, e.getType());
      Assert.assertEquals(
          "check error message", "Synonym document field limit exceeded: 6 > 4", e.getMessage());
      mocks.documentIndexer.completeExceptionally(e);
      verify(mocks.synonymRegistry).invalidate(eq(MOCK_SYNONYM_MAPPING_DEFINITION_NAME), any());
      verifyNoMoreInteractions(mocks.builder, mocks.synonymRegistry);
      return;
    }

    Assert.fail("should have thrown exception");
  }

  private static Collection<RawBsonDocument> invalidDocuments() {
    return List.of(
        BsonUtils.documentToRaw(
            new BsonDocument(
                List.of(new BsonElement("mappingType", new BsonString("equivalent"))))),
        BsonUtils.documentToRaw(
            new BsonDocument(
                List.of(
                    new BsonElement(
                        "synonyms",
                        new BsonArray(List.of(new BsonString("car"), new BsonString("truck"))))))));
  }

  private static Collection<RawBsonDocument> validDocuments() {
    return List.of(
        validEquivalentDocEvent(List.of("car", "truck", "sedan")),
        validExplicitDocEvent(List.of("beer"), List.of("brew", "pint")),
        validExplicitDocEvent(List.of("light"), List.of("bulb", "bright")));
  }

  private static RawBsonDocument validEquivalentDocEvent(List<String> synonyms) {
    return SynonymDocumentBuilder.equivalent(synonyms).toRawBson();
  }

  private static RawBsonDocument validExplicitDocEvent(List<String> inputs, List<String> synonyms) {
    return SynonymDocumentBuilder.explicit(inputs, synonyms).toRawBson();
  }

  static class Mocks {
    final SynonymRegistry synonymRegistry;
    final SynonymMappingDefinition definition;
    final SynonymMapping.Builder builder;

    final SynonymDocumentIndexer documentIndexer;

    Mocks(
        SynonymRegistry synonymRegistry,
        SynonymMappingDefinition definition,
        SynonymMapping.Builder builder,
        Optional<Integer> maxDocsPerMappingLimit) {
      this.synonymRegistry = synonymRegistry;
      this.definition = definition;
      this.builder = builder;

      this.documentIndexer =
          new SynonymDocumentIndexer(
              this.synonymRegistry,
              this.definition,
              maxDocsPerMappingLimit.map(SynonymDocumentIndexer.Limits::new));
    }

    static Mocks create() throws Exception {
      return create(InvocationOnMock::getMock, Optional.empty());
    }

    static Mocks create(int maxDocsPerMapping) throws Exception {
      return create(InvocationOnMock::getMock, Optional.of(maxDocsPerMapping));
    }

    private static Mocks create(Answer<?> addDocumentAnswer, Optional<Integer> maxDocsPerMapping)
        throws Exception {

      SynonymRegistry registry = mock(SynonymRegistry.class);

      SynonymMapping.Builder builder = mock(SynonymMapping.Builder.class);
      doAnswer(addDocumentAnswer).when(builder).addDocument(any());

      when(registry.mappingBuilder(any())).thenReturn(builder);

      return new Mocks(
          registry, MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION, builder, maxDocsPerMapping);
    }
  }
}
