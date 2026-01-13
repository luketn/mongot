package com.xgen.mongot.server.command.management.aic;

import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.COLLECTION_UUID;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.DATABASE_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.INDEX_NAME;
import static com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder.VIEW;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AicListSearchIndexesCommandTest {
  @Test
  public void testListSearchIndex() {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .database(DATABASE_NAME)
            .collectionUuid(COLLECTION_UUID)
            .lastObservedCollectionName(COLLECTION_NAME)
            .indexId(new ObjectId())
            .name(INDEX_NAME)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    when(mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation().buildSearchIndexCommand();
    var command =
        new AicListSearchIndexesCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
    assertEquals("search", batch.getFirst().asDocument().getString("type").getValue());
  }

  @Test
  public void testListSearchIndexFiltering() {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .database(DATABASE_NAME)
            .collectionUuid(COLLECTION_UUID)
            .lastObservedCollectionName(COLLECTION_NAME)
            .indexId(new ObjectId())
            .name(INDEX_NAME)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    when(mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation()
                .withIndexName(INDEX_NAME)
                .buildSearchIndexCommand();
    var command =
        new AicListSearchIndexesCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
    assertEquals("search", batch.getFirst().asDocument().getString("type").getValue());
  }

  @Test
  public void testListSearchIndexFilteringNoMatch() {
    var mockAic = mock(AuthoritativeIndexCatalog.class);
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .database(DATABASE_NAME)
            .collectionUuid(COLLECTION_UUID)
            .lastObservedCollectionName(COLLECTION_NAME)
            .indexId(new ObjectId())
            .name(INDEX_NAME)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    when(mockAic.listIndexes(COLLECTION_UUID)).thenReturn(List.of(indexDefinition));

    var definition =
        (ListSearchIndexesCommandDefinition)
            ManageSearchIndexCommandDefinitionBuilder.listAggregation()
                .withIndexName("nonexistent")
                .buildSearchIndexCommand();
    var command =
        new AicListSearchIndexesCommand(
            mockAic,
            DATABASE_NAME,
            COLLECTION_UUID,
            COLLECTION_NAME,
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();
    assertEquals(0, response.getDocument("cursor").getArray("firstBatch").size());
  }
}
