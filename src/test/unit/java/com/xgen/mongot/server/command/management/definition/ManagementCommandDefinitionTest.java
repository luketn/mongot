package com.xgen.mongot.server.command.management.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec.valid;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinition.Type;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite.TestSpec;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.ManageSearchIndexCommandDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserSearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserVectorIndexDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ManagementCommandDefinitionTest.TestDeserialization.class,
  ManagementCommandDefinitionTest.TestSerialization.class
})
public class ManagementCommandDefinitionTest {
  static final String RESOURCES_PATH =
      "src/test/unit/resources/server/command/management/definition/";

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "management-deserialization";
    private static final BsonDeserializationTestSuite<ManageSearchIndexCommandDefinition>
        TEST_SUITE =
            fromDocument(RESOURCES_PATH, SUITE_NAME, ManageSearchIndexCommandDefinition::fromBson);

    private final TestSpecWrapper<ManageSearchIndexCommandDefinition> testSpec;

    public TestDeserialization(TestSpecWrapper<ManageSearchIndexCommandDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<ManageSearchIndexCommandDefinition>> data() {
      return TEST_SUITE.withExamples(
          create(),
          createVector(),
          drop(),
          listCommand(),
          listAggregation(),
          update(),
          updateVectorByName(),
          updateVectorById());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> update() {
      return valid(
          "update",
          ManageSearchIndexCommandDefinitionBuilder.updateIndex()
              .searchIndex("myIndex")
              .view(new UserViewDefinition("testView", Optional.empty()))
              .withDefinition(
                  UserSearchIndexDefinitionBuilder.builder()
                      .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
                      .build())
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> updateVectorByName() {
      return valid(
          "update vector by name",
          ManageSearchIndexCommandDefinitionBuilder.updateIndex()
              .vectorIndex("myNewVectorIndex")
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .withDefinition(
                  UserVectorIndexDefinitionBuilder.builder()
                      .addVectorField(
                          2048, VectorSimilarity.COSINE, VectorQuantization.NONE, "my.vector.field")
                      .addFilterField("my.filter.field")
                      .build())
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> updateVectorById() {
      return valid(
          "update vector by id",
          ManageSearchIndexCommandDefinitionBuilder.updateIndex()
              .withIndexId(new ObjectId("000000000000000000000000"))
              .view(new UserViewDefinition("testView", Optional.empty()))
              .ofType(IndexDefinition.Type.VECTOR_SEARCH)
              .withDefinition(
                  UserVectorIndexDefinitionBuilder.builder()
                      .addVectorField(
                          2048, VectorSimilarity.COSINE, VectorQuantization.NONE, "my.vector.field")
                      .addFilterField("my.filter.field")
                      .build())
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> create() {
      return valid(
          "create",
          ManageSearchIndexCommandDefinitionBuilder.createIndexes()
              .withDynamicIndex()
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> createVector() {
      return valid(
          "create vector",
          ManageSearchIndexCommandDefinitionBuilder.createIndexes()
              .addIndex(
                  new NamedSearchIndex(
                      "myVectorIndex",
                      IndexDefinition.Type.VECTOR_SEARCH,
                      UserVectorIndexDefinitionBuilder.builder()
                          .addVectorField(
                              2048,
                              VectorSimilarity.COSINE,
                              VectorQuantization.NONE,
                              "my.vector.field")
                          .addFilterField("my.filter.field")
                          .build()))
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> drop() {
      return valid(
          "drop",
          ManageSearchIndexCommandDefinitionBuilder.dropIndex()
              .withIndexName("myIndex")
              .view(new UserViewDefinition("testView", Optional.empty()))
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> listCommand() {
      return valid(
          "list command",
          ManageSearchIndexCommandDefinitionBuilder.listCommand()
              .view(new UserViewDefinition("testView", Optional.empty()))
              .build());
    }

    private static ValidSpec<ManageSearchIndexCommandDefinition> listAggregation() {
      return valid(
          "list aggregation",
          ManageSearchIndexCommandDefinitionBuilder.listAggregation()
              .view(new UserViewDefinition("testView", Optional.empty()))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "management-serialization";
    private static final BsonSerializationTestSuite<ManageSearchIndexCommandDefinition> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final TestSpec<ManageSearchIndexCommandDefinition> testSpec;

    public TestSerialization(TestSpec<ManageSearchIndexCommandDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpec<ManageSearchIndexCommandDefinition>> data() {
      return Arrays.asList(
          createSearch(),
          createVector(),
          createSearchPartitioned(),
          updateSearch(),
          updateVector(),
          drop(),
          list());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> createSearch() {
      return TestSpec.create(
          "createSearch",
          ManageSearchIndexCommandDefinitionBuilder.createIndexes()
              .withDynamicIndex()
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> createVector() {
      return TestSpec.create(
          "createVector",
          ManageSearchIndexCommandDefinitionBuilder.createIndexes()
              .withVectorIndex()
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> createSearchPartitioned() {
      return TestSpec.create(
          "createSearchPartitioned",
          ManageSearchIndexCommandDefinitionBuilder.createIndexes()
              .addIndex(
                  new NamedSearchIndex(
                      ManageSearchIndexCommandDefinitionBuilder.INDEX_NAME,
                      Type.SEARCH,
                      new UserSearchIndexDefinition(
                          Optional.empty(),
                          Optional.empty(),
                          DocumentFieldDefinitionBuilder.builder().dynamic(true).build(),
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty(),
                          4)))
              .view(
                  new UserViewDefinition(
                      "testView",
                      Optional.of(
                          List.of(
                              new BsonDocument(
                                  "$addFields",
                                  new BsonDocument("name", new BsonString("value")))))))
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> updateSearch() {
      return TestSpec.create(
          "updateSearch",
          ManageSearchIndexCommandDefinitionBuilder.updateIndex()
              .searchIndex("myIndex")
              .view(new UserViewDefinition("testView", Optional.empty()))
              .withDefinition(
                  UserSearchIndexDefinitionBuilder.builder()
                      .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
                      .build())
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> updateVector() {
      return TestSpec.create(
          "updateVector",
          ManageSearchIndexCommandDefinitionBuilder.updateIndex()
              .withIndexId(new ObjectId("000000000000000000000000"))
              .view(new UserViewDefinition("testView", Optional.empty()))
              .ofType(IndexDefinition.Type.VECTOR_SEARCH)
              .withDefinition(
                  UserVectorIndexDefinitionBuilder.builder()
                      .addVectorField(
                          2048, VectorSimilarity.COSINE, VectorQuantization.NONE, "my.vector.field")
                      .addFilterField("my.filter.field")
                      .build())
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> drop() {
      return TestSpec.create(
          "drop",
          ManageSearchIndexCommandDefinitionBuilder.dropIndex()
              .withIndexName("myIndex")
              .view(new UserViewDefinition("testView", Optional.empty()))
              .build());
    }

    private static TestSpec<ManageSearchIndexCommandDefinition> list() {
      return TestSpec.create(
          "list",
          ManageSearchIndexCommandDefinitionBuilder.listAggregation()
              .view(new UserViewDefinition("testView", Optional.empty()))
              .build());
    }
  }
}
