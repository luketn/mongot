package com.xgen.mongot.index.lucene.query.sort;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class IndexSortUtilsTest {

  @Test
  public void testExtractFirstIndexSort_WithSort() throws IOException {
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));
    
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setIndexSort(indexSort);
      
      try (IndexWriter writer = new IndexWriter(directory, config)) {
        Document doc = new Document();
        doc.add(new LongPoint("date", 20231201L));
        doc.add(new NumericDocValuesField("date", 20231201L));
        writer.addDocument(doc);
      }
      
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Optional<Sort> result = IndexSortUtils.extractFirstIndexSort(reader);
        
        assertThat(result).isPresent();
        assertEquals(1, result.get().getSort().length);
        assertEquals("date", result.get().getSort()[0].getField());
        assertEquals(SortField.Type.LONG, result.get().getSort()[0].getType());
      }
    }
  }

  @Test
  public void testExtractFirstIndexSort_NoSort() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new LongPoint("date", 20231201L));
        doc.add(new NumericDocValuesField("date", 20231201L));
        writer.addDocument(doc);
      }
      
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Optional<Sort> result = IndexSortUtils.extractFirstIndexSort(reader);
        assertFalse(result.isPresent());
      }
    }
  }

  @Test
  public void testCanBenefitFromIndexSort_ExactMatch() {
    Sort querySort = new Sort(new SortField("date", SortField.Type.LONG));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertThat(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testCanBenefitFromIndexSort_QueryIsPrefix() {
    Sort querySort = new Sort(new SortField("date", SortField.Type.LONG));
    Sort indexSort = new Sort(
        new SortField("date", SortField.Type.LONG),
        new SortField("score", SortField.Type.SCORE));

    assertThat(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testCanBenefitFromIndexSort_QueryLongerThanIndex() {
    Sort querySort = new Sort(
        new SortField("date", SortField.Type.LONG),
        new SortField("score", SortField.Type.SCORE));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertFalse(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort));
  }

  @Test
  public void testCanBenefitFromIndexSort_DifferentFields() {
    Sort querySort = new Sort(new SortField("name", SortField.Type.STRING));
    Sort indexSort = new Sort(new SortField("date", SortField.Type.LONG));

    assertFalse(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort));
  }

  @Test
  public void testCanBenefitFromIndexSort_MqlLongSort_SameOrder() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());
    MqlLongSort indexField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertThat(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testCanBenefitFromIndexSort_MqlLongSort_DifferentOrder() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_DESC),
        true,
        Optional.empty());
    MqlLongSort indexField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("score"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort));
  }

  @Test
  public void testCanBenefitFromIndexSort_MqlDateSort_SameOrder() {
    MqlDateSort queryField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertThat(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort)).isTrue();
  }

  @Test
  public void testCanBenefitFromIndexSort_MqlDateSort_DifferentOrder() {
    MqlDateSort queryField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("date"), UserFieldSortOptions.DEFAULT_DESC),
        true,
        Optional.empty());

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort));
  }

  @Test
  public void testCanBenefitFromIndexSort_MixedMqlTypes_Incompatible() {
    MqlLongSort queryField = new MqlLongSort(
        FieldName.TypeField.NUMBER_INT64_V2,
        new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());
    MqlDateSort indexField = new MqlDateSort(
        FieldName.TypeField.DATE_V2,
        new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC),
        true,
        Optional.empty());

    Sort querySort = new Sort(queryField);
    Sort indexSort = new Sort(indexField);

    assertFalse(IndexSortUtils.canBenefitFromIndexSort(querySort, indexSort));
  }

  // --- expandedSortAlignsWithIndexSort tests ---

  @Test
  public void expandedSortAligns_singleFieldAligned_returnsTrue() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_multipleFieldsAligned_returnsTrue() {
    List<MongotSortField> queryFields =
        List.of(userSortField("a"), userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_queryPrefixOfIndex_returnsTrue() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_misalignedValuePosition_returnsFalse() {
    List<MongotSortField> queryFields = List.of(userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"),
        nullnessSortField("b"),
        valueSortField("$type:int64V2/b"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  @Test
  public void expandedSortAligns_expandedExceedsIndexLength_returnsFalse() {
    List<MongotSortField> queryFields =
        List.of(userSortField("a"), userSortField("b"));
    Sort indexSort = new Sort(
        nullnessSortField("a"),
        valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  @Test
  public void expandedSortAligns_noNullnessInIndex_valueFieldStillAligned() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(valueSortField("$type:int64V2/a"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_mixedTypesWithTokenField_returnsTrue() {
    List<MongotSortField> queryFields =
        List.of(userSortField("num"), userSortField("name"));
    Sort indexSort = new Sort(
        nullnessSortField("num"),
        valueSortField("$type:int64V2/num"),
        valueSortField("$type:token/name"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isTrue();
  }

  @Test
  public void expandedSortAligns_indexFieldWithoutTypePrefix_returnsFalse() {
    List<MongotSortField> queryFields = List.of(userSortField("a"));
    Sort indexSort = new Sort(valueSortField("plain_field_no_type"));

    assertThat(IndexSortUtils.expandedSortAlignsWithIndexSort(
        queryFields, indexSort))
        .isFalse();
  }

  private static MongotSortField userSortField(String path) {
    return new MongotSortField(
        FieldPath.newRoot(path), UserFieldSortOptions.DEFAULT_ASC);
  }

  private static SortedNumericSortField nullnessSortField(String path) {
    return new SortedNumericSortField(
        "$meta/nullness/" + path,
        SortField.Type.LONG,
        false,
        SortedNumericSelector.Type.MIN);
  }

  private static SortField valueSortField(String luceneFieldName) {
    return new SortedNumericSortField(
        luceneFieldName,
        SortField.Type.LONG,
        false,
        SortedNumericSelector.Type.MIN);
  }

}
