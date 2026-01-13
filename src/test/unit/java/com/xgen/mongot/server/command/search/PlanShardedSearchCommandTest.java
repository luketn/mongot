package com.xgen.mongot.server.command.search;

import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.testing.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinitionBuilder;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlanShardedSearchCommandTest {

  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";

  private static final BsonDocument VALID_OPERATOR_QUERY =
      new BsonDocument()
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static PlanShardedSearchCommandDefinition getPlanShardedSearchCommandDefinition(
      BsonDocument query) {
    return PlanShardedSearchCommandDefinitionBuilder.builder()
        .db(DATABASE_NAME)
        .collectionName(COLLECTION_NAME)
        .query(query)
        .build();
  }

  @Test
  public void testValidQuery() {
    PlanShardedSearchCommand command =
        new PlanShardedSearchCommand(getPlanShardedSearchCommandDefinition(VALID_OPERATOR_QUERY));

    List<BsonDocument> metaPipeline =
        List.of(
            new BsonDocument(
                "$group",
                new BsonDocument()
                    .append(
                        "_id",
                        new BsonDocument()
                            .append("type", new BsonString("$type"))
                            .append("tag", new BsonString("$tag"))
                            .append("bucket", new BsonString("$bucket")))
                    .append("value", new BsonDocument("$sum", new BsonString("$count")))),
            new BsonDocument(
                "$facet",
                new BsonDocument()
                    .append(
                        "count",
                        new BsonArray(
                            List.of(
                                new BsonDocument(
                                    "$match",
                                    new BsonDocument(
                                        "_id.type",
                                        new BsonDocument("$eq", new BsonString("count")))))))),
            new BsonDocument(
                "$replaceWith",
                new BsonDocument()
                    .append(
                        "count",
                        new BsonDocument(
                            "lowerBound",
                            new BsonDocument("$first", new BsonString("$count.value"))))));

    BsonDocument expected =
        PlanShardedSearchCommandResponseDefinition.create(
                new PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan(
                    metaPipeline,
                    PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan.DEFAULT_SORT_SPEC))
            .toBson();
    BsonDocument actual = command.run();
    Assert.assertEquals(expected, actual);
  }
}
