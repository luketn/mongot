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
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AicExtendedListSearchIndexesCommandTest {
  @Test
  public void testListSearchIndexAllIndexes() {
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
    when(mockAic.listIndexes()).thenReturn(List.of(indexDefinition));

    var mockIndexInfoProvider = mock(CachedIndexInfoProvider.class);
    var indexInformation =
        new IndexInformation.Search(
            indexDefinition,
            IndexStatus.initialSync(),
            List.of(),
            new AggregatedIndexMetrics(0, 1234L, new BsonTimestamp(0), 0),
            Optional.empty(),
            Optional.empty(),
            new HashMap<>());
    when(mockIndexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInformation));

    var definition =
        new ListSearchIndexesCommandDefinition(
            new ListSearchIndexesCommandDefinition.ListTarget(Optional.empty(), Optional.empty()));
    var command =
        new AicExtendedListSearchIndexesCommand(
            mockAic,
            mockIndexInfoProvider,
            "__mdb_internal_search",
            COLLECTION_UUID,
            "indexCatalog",
            Optional.of(VIEW),
            definition);

    BsonDocument response = command.run();
    BsonArray batch = response.getDocument("cursor").getArray("firstBatch");

    assertEquals(1, response.getInt32("ok").getValue());
    assertEquals(1, batch.size());
    assertEquals(INDEX_NAME, batch.getFirst().asDocument().getString("name").getValue());
    assertEquals("search", batch.getFirst().asDocument().getString("type").getValue());
    assertEquals("BUILDING", batch.getFirst().asDocument().getString("status").getValue());
    assertEquals(1234L, batch.getFirst().asDocument().getInt64("numDocs").getValue());
  }
}
