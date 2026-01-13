package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableDateBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableNumberBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableStringBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.TriFunction;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class SearchFieldDefinitionResolverTest {
  @DataPoints
  public static IndexFormatVersion[] indexFormatVersions() {
    return new IndexFormatVersion[] {
      IndexFormatVersion.MIN_SUPPORTED_VERSION, IndexFormatVersion.CURRENT
    };
  }

  public record DynamicFieldDefinitionData(
      DynamicDefinition dynamicDefinition,
      FieldDefinition correspondingFieldDefinition,
      Optional<List<TypeSetDefinition>> typeSets) {}

  @DataPoints
  public static DynamicFieldDefinitionData[] dynamicFieldDefinitionData() {
    return new DynamicFieldDefinitionData[] {
      new DynamicFieldDefinitionData(
          new DynamicDefinition.Boolean(true),
          FieldDefinition.DYNAMIC_FIELD_DEFINITION,
          Optional.empty()),
      new DynamicFieldDefinitionData(
          new DynamicDefinition.Document("test"),
          FieldDefinitionBuilder.builder()
              .token(TokenFieldDefinitionBuilder.builder().build())
              .autocomplete(AutocompleteFieldDefinitionBuilder.builder().build())
              .document(DocumentFieldDefinitionBuilder.builder().dynamic("test").build())
              .build(),
          Optional.of(
              List.of(
                  TypeSetDefinitionBuilder.builder()
                      .name("test")
                      .addType(TokenFieldDefinitionBuilder.builder().build())
                      .addType(AutocompleteFieldDefinitionBuilder.builder().build())
                      .build())))
    };
  }

  @Theory
  public void getFieldDefinitionDynamicDocument_noMatchingTypeSets_throwsException(
      IndexFormatVersion indexFormatVersion) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .typeSets(
                TypeSetDefinitionBuilder.builder()
                    .name("test")
                    .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
                    .build())
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic("foo").build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);
    AssertionError e =
        Assert.assertThrows(
            AssertionError.class,
            () ->
                fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a"), Optional.empty()));
    Assert.assertEquals("matchingTypeSet must be present", e.getMessage());
  }

  @Theory
  public void testDynamicFieldDefinition(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);
    var field = fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());
  }

  @Theory
  public void testGetFieldsNotStaticallyConfiguredWithDynamicRoot(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());

    var embeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("foo")));
    Assert.assertTrue(embeddedField.isEmpty());
  }

  @Theory
  public void testEmbeddedGetFieldsNotStaticallyConfiguredWithDynamicRoot(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    FieldDefinition teachersDotFirstName =
        FieldDefinitionBuilder.builder()
            .string(StringFieldDefinitionBuilder.builder().analyzerName("lucene.french").build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "teachers",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .field("firstName", teachersDotFirstName)
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .build())
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());

    var dynamicField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.lastName"), Optional.empty());
    Assert.assertTrue(dynamicField.isPresent());
    Assert.assertEquals(
        dynamicFieldDefinitionData.correspondingFieldDefinition(), dynamicField.get());

    var embeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.of(FieldPath.parse("teachers")));
    Assert.assertTrue(embeddedField.isPresent());
    Assert.assertEquals(teachersDotFirstName, embeddedField.get());

    var dynamicEmbeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.lastName"), Optional.of(FieldPath.parse("teachers")));
    Assert.assertTrue(dynamicEmbeddedField.isPresent());
    Assert.assertEquals(
        dynamicFieldDefinitionData.correspondingFieldDefinition(), dynamicEmbeddedField.get());

    var notEmbeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.of(FieldPath.parse("foo")));
    Assert.assertTrue(notEmbeddedField.isEmpty());
  }

  @Theory
  public void testEmbeddedGetFieldsNotStaticallyConfiguredWithNonDynamicRoot(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    FieldDefinition teachersDotFirstName =
        FieldDefinitionBuilder.builder()
            .string(StringFieldDefinitionBuilder.builder().analyzerName("lucene.french").build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "teachers",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .field("firstName", teachersDotFirstName)
                                    .dynamic(false)
                                    .build())
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());

    var dynamicField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.lastName"), Optional.empty());
    Assert.assertTrue(dynamicField.isPresent());
    Assert.assertEquals(
        dynamicFieldDefinitionData.correspondingFieldDefinition(), dynamicField.get());

    var embeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.of(FieldPath.parse("teachers")));
    Assert.assertTrue(embeddedField.isPresent());
    Assert.assertEquals(teachersDotFirstName, embeddedField.get());

    var notDynamicEmbeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.lastName"), Optional.of(FieldPath.parse("teachers")));
    Assert.assertTrue(notDynamicEmbeddedField.isEmpty());

    var notEmbeddedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("teachers.firstName"), Optional.of(FieldPath.parse("foo")));
    Assert.assertTrue(notEmbeddedField.isEmpty());
  }

  @Theory
  public void testGetFieldNotStaticallyConfiguredWithDynamicAncestor(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());
  }

  @Theory
  public void testGetEmbeddedFieldNotStaticallyConfiguredWithDynamicAncestor(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .dynamic(false)
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .document(
                                                DocumentFieldDefinitionBuilder.builder()
                                                    .dynamic(
                                                        dynamicFieldDefinitionData
                                                            .dynamicDefinition())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), field.get());
  }

  @Theory
  public void testGetFieldNotStaticallyConfiguredWithNonDynamicAncestor(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .document(
                                DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isEmpty());
  }

  @Theory
  public void testGetEmbeddedFieldNotStaticallyConfiguredWithNonDynamicAncestor(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .document(
                                                DocumentFieldDefinitionBuilder.builder()
                                                    .dynamic(false)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(field.isEmpty());

    var fieldAcc =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.c.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(fieldAcc.isPresent());
    Assert.assertEquals(dynamicFieldDefinitionData.correspondingFieldDefinition(), fieldAcc.get());
  }

  @Theory
  public void
      testGetFieldNotStaticallyConfiguredWithNonDynamicDirectAncestorAndDynamicHigherAncestor(
          IndexFormatVersion indexFormatVersion) {
    // This test does not use DynamicFieldDefinitionData and tests a specific case with
    // dynamic(true), so it remains unchanged.
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .dynamic(true)
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .string(StringFieldDefinitionBuilder.builder().build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isEmpty());
  }

  @Theory
  public void testEmbeddedGetFieldNotStaticConfWithNonDynamicDirectAncestorAndDynamicHigherAncestor(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .typeSets(
                dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream).toList())
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .string(StringFieldDefinitionBuilder.builder().build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(field.isEmpty());
  }

  @Theory
  public void getNonEmbeddedDynamicField_nestedDynamicTypeSet_getsCorrectFieldDefinition(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    var typeSetDef =
        TypeSetDefinitionBuilder.builder()
            .name("nestedDynamicTypeSet")
            .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .addType(StringFieldDefinitionBuilder.builder().build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .typeSets(
                Stream.concat(
                        dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream),
                        Stream.of(typeSetDef))
                    .toList())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .document(
                                                DocumentFieldDefinitionBuilder.builder()
                                                    .dynamic("nestedDynamicTypeSet")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(typeSetDef.getFieldDefinition(), field.get());

    var altNestedField =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.d"), Optional.empty());
    Assert.assertTrue(altNestedField.isPresent());
    Assert.assertEquals(
        dynamicFieldDefinitionData.correspondingFieldDefinition(), altNestedField.get());
  }

  @Theory
  public void getEmbeddedDynamicField_nestedDynamicTypeSet_getsCorrectFieldDefinition(
      IndexFormatVersion indexFormatVersion,
      DynamicFieldDefinitionData dynamicFieldDefinitionData) {
    var typeSetDef =
        TypeSetDefinitionBuilder.builder()
            .name("nestedDynamicTypeSet")
            .addType(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .addType(StringFieldDefinitionBuilder.builder().build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .typeSets(
                Stream.concat(
                        dynamicFieldDefinitionData.typeSets().stream().flatMap(Collection::stream),
                        Stream.of(typeSetDef))
                    .toList())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .dynamic(dynamicFieldDefinitionData.dynamicDefinition())
                                    .field(
                                        "b",
                                        FieldDefinitionBuilder.builder()
                                            .document(
                                                DocumentFieldDefinitionBuilder.builder()
                                                    .dynamic("nestedDynamicTypeSet")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(typeSetDef.getFieldDefinition(), field.get());

    var altNestedField =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.d"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(altNestedField.isPresent());
    Assert.assertEquals(
        dynamicFieldDefinitionData.correspondingFieldDefinition(), altNestedField.get());
  }

  @Theory
  public void testGetFieldStaticallyConfigured(IndexFormatVersion indexFormatVersion) {
    FieldDefinition c =
        FieldDefinitionBuilder.builder().geo(GeoFieldDefinitionBuilder.builder().build()).build();

    FieldDefinition b =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().dynamic(false).field("c", c).build())
            .build();

    FieldDefinition a =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().dynamic(false).field("b", b).build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).field("a", a).build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var field =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(field.isPresent());
    Assert.assertEquals(c, field.get());
  }

  @Theory
  public void testGetEmbeddedFieldStaticallyConfigured(IndexFormatVersion indexFormatVersion) {
    FieldDefinition c =
        FieldDefinitionBuilder.builder().geo(GeoFieldDefinitionBuilder.builder().build()).build();

    FieldDefinition b =
        FieldDefinitionBuilder.builder()
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field("c", c)
                    .build())
            .build();

    FieldDefinition a =
        FieldDefinitionBuilder.builder()
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field("b", b)
                    .build())
            .build();

    SearchIndexDefinition definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).field("a", a).build())
            .build();

    SearchFieldDefinitionResolver fieldDefinitionResolver =
        new SearchFieldDefinitionResolver(definition, indexFormatVersion);

    var fieldANotEmbedded =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a"), Optional.empty());
    Assert.assertTrue(fieldANotEmbedded.isPresent());
    Assert.assertEquals(a, fieldANotEmbedded.get());

    var fieldAEmbeddedA =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(fieldAEmbeddedA.isEmpty());

    var fieldAEmbeddedAb =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a"), Optional.of(FieldPath.parse("a.b")));
    Assert.assertTrue(fieldAEmbeddedAb.isEmpty());

    var fieldAbNotEmbedded =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b"), Optional.empty());
    Assert.assertTrue(fieldAbNotEmbedded.isEmpty());

    var fieldAbEmbeddedA =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(fieldAbEmbeddedA.isPresent());
    Assert.assertEquals(b, fieldAbEmbeddedA.get());

    var fieldAbEmbeddedAb =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b"), Optional.of(FieldPath.parse("a.b")));
    Assert.assertTrue(fieldAbEmbeddedAb.isEmpty());

    var fieldAbcNotEmbedded =
        fieldDefinitionResolver.getFieldDefinition(FieldPath.parse("a.b.c"), Optional.empty());
    Assert.assertTrue(fieldAbcNotEmbedded.isEmpty());

    var fieldAbcEmbeddedA =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a")));
    Assert.assertTrue(fieldAbcEmbeddedA.isEmpty());

    var fieldAbcEmbeddedAb =
        fieldDefinitionResolver.getFieldDefinition(
            FieldPath.parse("a.b.c"), Optional.of(FieldPath.parse("a.b")));
    Assert.assertTrue(fieldAbcEmbeddedAb.isPresent());
    Assert.assertEquals(c, fieldAbcEmbeddedAb.get());
  }

  @Theory
  public void testGetAutocompleteField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        AutocompleteFieldDefinitionBuilder.builder().foldDiacritics(false).build(),
        FieldDefinitionBuilder::autocomplete,
        FieldDefinition::autocompleteFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testBooleanFieldIfv5(IndexFormatVersion indexFormatVersion) {
    Assume.assumeTrue(indexFormatVersion.versionNumber >= IndexFormatVersion.FIVE.versionNumber);
    assertGetFieldDefinition(
        BooleanFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::bool,
        FieldDefinition::booleanFieldDefinition,
        true,
        indexFormatVersion);
  }

  @Theory
  public void testObjectIdFieldIfv5(IndexFormatVersion indexFormatVersion) {
    Assume.assumeTrue(indexFormatVersion.versionNumber >= IndexFormatVersion.FIVE.versionNumber);
    assertGetFieldDefinition(
        ObjectIdFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::objectid,
        FieldDefinition::objectIdFieldDefinition,
        true,
        indexFormatVersion);
  }

  @Theory
  public void testGetDateField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        DateFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::date,
        FieldDefinition::dateFieldDefinition,
        true,
        indexFormatVersion);
  }

  @Theory
  public void testGetDateFacetField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        DateFacetFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::dateFacet,
        FieldDefinition::dateFacetFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testGetDocumentField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        DocumentFieldDefinitionBuilder.builder().dynamic(false).build(),
        FieldDefinitionBuilder::document,
        FieldDefinition::documentFieldDefinition,
        true,
        indexFormatVersion);
  }

  @Theory
  public void testGetGeoField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        GeoFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::geo,
        FieldDefinition::geoFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testGetNumberField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        NumericFieldDefinitionBuilder.builder().buildNumberField(),
        FieldDefinitionBuilder::number,
        FieldDefinition::numberFieldDefinition,
        true,
        indexFormatVersion);
  }

  @Theory
  public void testGetNumberFacetField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        NumericFieldDefinitionBuilder.builder().buildNumberFacetField(),
        FieldDefinitionBuilder::numberFacet,
        FieldDefinition::numberFacetFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testSortableDateBetaV1(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        SortableDateBetaV1FieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::sortableDateBetaV1,
        FieldDefinition::sortableDateBetaV1FieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testSortableStringBetaV1(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        SortableStringBetaV1FieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::sortableStringBetaV1,
        FieldDefinition::sortableStringBetaV1FieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testSortableNumberBetaV1(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        SortableNumberBetaV1FieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::sortableNumberBetaV1,
        FieldDefinition::sortableNumberBetaV1FieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testKnnVectorField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        KnnVectorFieldDefinitionBuilder.builder()
            .dimensions(5)
            .similarity(VectorSimilarity.COSINE)
            .build(),
        FieldDefinitionBuilder::knnVector,
        FieldDefinition::knnVectorFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testGetStringField(IndexFormatVersion indexFormatVersion) {
    assertGetStringFieldDefinition(
        StringFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::string,
        FieldDefinition::stringFieldDefinition,
        indexFormatVersion);
  }

  @Theory
  public void testGetStringFacetField(IndexFormatVersion indexFormatVersion) {
    assertGetFieldDefinition(
        StringFacetFieldDefinitionBuilder.builder().build(),
        FieldDefinitionBuilder::stringFacet,
        FieldDefinition::stringFacetFieldDefinition,
        false,
        indexFormatVersion);
  }

  @Theory
  public void testGetStringMultiField(IndexFormatVersion indexFormatVersion) {
    SearchIndexDefinition notConfiguredNotDynamic =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .build();
    SearchFieldDefinitionResolver nonConfiguredNotDynamicResolver =
        new SearchFieldDefinitionResolver(notConfiguredNotDynamic, indexFormatVersion);
    Assert.assertTrue(
        nonConfiguredNotDynamicResolver
            .getStringFieldDefinition(StringPathBuilder.withMulti("a", "multi"), Optional.empty())
            .isEmpty());

    SearchIndexDefinition notConfiguredDynamic =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    SearchFieldDefinitionResolver notConfiguredDynamicResolver =
        new SearchFieldDefinitionResolver(notConfiguredDynamic, indexFormatVersion);
    Assert.assertTrue(
        notConfiguredDynamicResolver
            .getStringFieldDefinition(StringPathBuilder.withMulti("a", "multi"), Optional.empty())
            .isEmpty());

    SearchIndexDefinition stringConfiguredNoMulti =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    SearchFieldDefinitionResolver stringConfiguredNoMultiResolver =
        new SearchFieldDefinitionResolver(stringConfiguredNoMulti, indexFormatVersion);
    Assert.assertTrue(
        stringConfiguredNoMultiResolver
            .getStringFieldDefinition(StringPathBuilder.withMulti("a", "multi"), Optional.empty())
            .isEmpty());

    StringFieldDefinition multi =
        StringFieldDefinitionBuilder.builder().analyzerName("my-multi-analyzer").build();
    SearchIndexDefinition multiConfigured =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .string(
                                StringFieldDefinitionBuilder.builder()
                                    .multi("multi", multi)
                                    .build())
                            .build())
                    .build())
            .build();
    SearchFieldDefinitionResolver multiConfiguredResolver =
        new SearchFieldDefinitionResolver(multiConfigured, indexFormatVersion);
    var result =
        multiConfiguredResolver.getStringFieldDefinition(
            StringPathBuilder.withMulti("a", "multi"), Optional.empty());
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(multi, result.get());

    var notPresentResult =
        multiConfiguredResolver.getStringFieldDefinition(
            StringPathBuilder.withMulti("a", "does-not-exist"), Optional.empty());
    Assert.assertTrue(notPresentResult.isEmpty());
  }

  private static <T extends FieldTypeDefinition> void assertGetFieldDefinition(
      T expected,
      BiFunction<FieldDefinitionBuilder, T, FieldDefinitionBuilder> builder,
      Function<FieldDefinition, Optional<T>> optionalMapper,
      boolean inDynamicDefinition,
      IndexFormatVersion indexFormatVersion) {
    assertGetFieldType(
        expected,
        builder,
        SearchFieldDefinitionResolver::getFieldDefinition,
        optionalMapper,
        inDynamicDefinition,
        indexFormatVersion);
  }

  private static <T extends FieldTypeDefinition> void assertGetStringFieldDefinition(
      T expected,
      BiFunction<FieldDefinitionBuilder, T, FieldDefinitionBuilder> builder,
      Function<FieldDefinition, Optional<T>> optionalMapper,
      IndexFormatVersion indexFormatVersion) {
    assertGetFieldType(
        expected,
        builder,
        (fieldDefinitionResolver, path, embeddedRoot) -> {
          Optional<StringFieldDefinition> maybeStringField =
              fieldDefinitionResolver.getStringFieldDefinition(
                  new StringFieldPath(path), embeddedRoot);
          if (maybeStringField.isEmpty()) {
            return Optional.empty();
          }

          return Optional.of(
              FieldDefinitionBuilder.builder().string(maybeStringField.get()).build());
        },
        optionalMapper,
        FieldDefinition.DYNAMIC_FIELD_DEFINITION.stringFieldDefinition().isPresent(),
        indexFormatVersion);
  }

  private static <T extends FieldTypeDefinition> void assertGetFieldType(
      T expected,
      BiFunction<FieldDefinitionBuilder, T, FieldDefinitionBuilder> builder,
      TriFunction<
              SearchFieldDefinitionResolver,
              FieldPath,
              Optional<FieldPath>,
              Optional<FieldDefinition>>
          fieldGetter,
      Function<FieldDefinition, Optional<T>> optionalMapper,
      boolean inDynamicDefinition,
      IndexFormatVersion indexFormatVersion) {
    FieldPath a = FieldPath.parse("a");

    SearchIndexDefinition notConfiguredNotDynamic =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .build();
    SearchFieldDefinitionResolver nonDynamicDefinitionResolver =
        new SearchFieldDefinitionResolver(notConfiguredNotDynamic, indexFormatVersion);
    Assert.assertTrue(
        fieldGetter
            .apply(nonDynamicDefinitionResolver, a, Optional.empty())
            .flatMap(optionalMapper)
            .isEmpty());

    SearchIndexDefinition notConfiguredDynamic =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    SearchFieldDefinitionResolver dynamicDefinitionResolver =
        new SearchFieldDefinitionResolver(notConfiguredDynamic, indexFormatVersion);
    Assert.assertEquals(
        inDynamicDefinition,
        fieldGetter
            .apply(dynamicDefinitionResolver, a, Optional.empty())
            .flatMap(optionalMapper)
            .isPresent());

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field("a", builder.apply(FieldDefinitionBuilder.builder(), expected).build())
                    .build())
            .build();
    SearchFieldDefinitionResolver indexResolver =
        new SearchFieldDefinitionResolver(index, indexFormatVersion);
    var result =
        fieldGetter
            .apply(indexResolver, FieldPath.parse("a"), Optional.empty())
            .flatMap(optionalMapper);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }
}
