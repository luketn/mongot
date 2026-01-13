package com.xgen.mongot.config.backup;

import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class JournalEditorTest {

  public record TestData(
      IndexDefinitionGeneration indexGen1,
      IndexDefinitionGeneration indexGen2,
      ConfigJournalV1 oneLiveIndex) {

    private static TestData create(IndexDefinitionGeneration indexDefinitionGeneration) {
      return new TestData(
          indexDefinitionGeneration,
          indexDefinitionGeneration.incrementAttempt(),
          ConfigJournalV1Builder.builder().liveIndex(indexDefinitionGeneration).build());
    }
  }

  @DataPoints("testData")
  public static List<TestData> clusterTypes() {
    return List.of(
        TestData.create(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION),
        TestData.create(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION));
  }

  private static final ConfigJournalV1 EMPTY = ConfigJournalV1Builder.builder().build();

  @Test
  public void testEmptyUnchanged() {
    assertJournalEquals(EMPTY, JournalEditor.on(EMPTY).journal());
  }

  @Theory
  public void testStagedIndexUnchanged(@FromDataPoints("testData") TestData data) {
    ConfigJournalV1 oneStaged =
        ConfigJournalV1Builder.builder().stagedIndex(data.indexGen1).build();
    assertJournalEquals(oneStaged, JournalEditor.on(oneStaged).journal());
  }

  @Theory
  public void testLiveIndexUnchanged(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(data.oneLiveIndex, JournalEditor.on(data.oneLiveIndex).journal());
  }

  @Theory
  public void testDeletedIndexUnchanged(@FromDataPoints("testData") TestData data) {
    ConfigJournalV1 oneDeleted =
        ConfigJournalV1Builder.builder().deletedIndex(data.indexGen1).build();
    assertJournalEquals(oneDeleted, JournalEditor.on(oneDeleted).journal());
  }

  @Theory
  public void testMovingEmptyListsDoesNothing(@FromDataPoints("testData") TestData data) {
    var input =
        ConfigJournalV1Builder.builder()
            .stagedIndex(data.indexGen1)
            .liveIndex(data.indexGen1)
            .build();
    var edited =
        JournalEditor.on(input)
            .fromLiveToDropped(Collections.emptyList())
            .fromStagedToLive(Collections.emptyList())
            .journal();
    assertJournalEquals(input, edited);
  }

  @Theory
  public void testEmptyAddedLive(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(
        ConfigJournalV1Builder.builder().liveIndex(data.indexGen1).build(),
        JournalEditor.on(EMPTY).addLive(List.of(data.indexGen1)).journal());
  }

  @Theory
  public void testAddingTwoIndexes(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build(),
        JournalEditor.on(EMPTY).addLive(List.of(data.indexGen1, data.indexGen2)).journal());
  }

  @Theory
  public void testMoveFromLiveAndAddNew(@FromDataPoints("testData") TestData data) {
    // drop INDEX_GEN_1 and add INDEX_GEN_2
    var expected =
        ConfigJournalV1Builder.builder()
            .deletedIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(data.oneLiveIndex)
            .addLive(List.of(data.indexGen2))
            .fromLiveToDropped(List.of(data.indexGen1.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);

    var actualReverseOrderOfOperations =
        JournalEditor.on(data.oneLiveIndex)
            .fromLiveToDropped(List.of(data.indexGen1.getGenerationId()))
            .addLive(List.of(data.indexGen2))
            .journal();
    assertJournalEquals(expected, actualReverseOrderOfOperations);
  }

  @Theory
  public void testDropTwoIndexes(@FromDataPoints("testData") TestData data) {
    var expected =
        ConfigJournalV1Builder.builder()
            .deletedIndex(data.indexGen1)
            .deletedIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(
                ConfigJournalV1Builder.builder()
                    .liveIndex(data.indexGen1)
                    .liveIndex(data.indexGen2)
                    .build())
            .fromLiveToDropped(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);
  }

  @Theory
  public void testFromStagedToLive(@FromDataPoints("testData") TestData data) {
    // INDEX_GEN_2 is being moved from being staged to being live
    var expected =
        ConfigJournalV1Builder.builder()
            .liveIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(
                ConfigJournalV1Builder.builder()
                    .liveIndex(data.indexGen1)
                    .stagedIndex(data.indexGen2)
                    .build())
            .fromStagedToLive(List.of(data.indexGen2.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);
  }

  @Theory
  public void testDroppingNonExistingIndexShouldThrow(@FromDataPoints("testData") TestData data) {
    var editor = JournalEditor.on(EMPTY);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> editor.fromLiveToDropped(List.of(data.indexGen1.getGenerationId())));
    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            editor.fromLiveToDropped(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId())));
  }

  @Theory
  public void testSwappingInNonExistingIndexShouldThrow(@FromDataPoints("testData") TestData data) {
    var editor = JournalEditor.on(EMPTY);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> editor.fromStagedToLive(List.of(data.indexGen1.getGenerationId())));
    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            editor.fromStagedToLive(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId())));
  }

  private void assertJournalEquals(ConfigJournalV1 expected, ConfigJournalV1 actual) {
    Assert.assertEquals(expected, actual);
  }
}
