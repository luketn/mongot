package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class FieldHierarchyContextTest {

  @Test
  public void testNumLayers() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = Map.of("foo", embeddedAndDocument());

    assertWithMessage("should have one embeddedDocuments layer when context is from document field")
        .that(FieldHierarchyContext.createForDocumentsField(fields).getNumEmbeddedDocumentsLayers())
        .isEqualTo(1);

    assertWithMessage(
            "should have two embeddedDocuments layer when context is from embeddedDocuments field")
        .that(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getNumEmbeddedDocumentsLayers())
        .isEqualTo(2);
  }

  @Test
  public void testNumLayersUnderDocument() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.of("foo", document("bar", document("baz", embedded())));

    assertWithMessage("should have one embeddedDocuments layer when context is from document field")
        .that(FieldHierarchyContext.createForDocumentsField(fields).getNumEmbeddedDocumentsLayers())
        .isEqualTo(1);

    assertWithMessage(
            "should have two embeddedDocuments layer when context is from embeddedDocuments field")
        .that(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getNumEmbeddedDocumentsLayers())
        .isEqualTo(2);
  }

  @Test
  public void testMultipleNestingSameLevel() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("foo", embeddedAndDocument()),
            Map.entry("bar", embeddedAndDocument()),
            Map.entry("baz", embeddedAndDocument()));

    assertWithMessage("should be max one layer deep")
        .that(FieldHierarchyContext.createForDocumentsField(fields).getNumEmbeddedDocumentsLayers())
        .isEqualTo(1);

    assertWithMessage("should be max two layer deep")
        .that(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getNumEmbeddedDocumentsLayers())
        .isEqualTo(2);
  }

  @Test
  public void testMaxOfDeepNesting() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("a", document("b", embedded("c", document("d", embedded())))),
            Map.entry("g", document("h", document("i", embedded("j", embedded("l", document()))))),
            Map.entry("m", embedded("n", embedded("o", embeddedAndDocument()))));

    assertWithMessage("should be max 3 layers deep")
        .that(FieldHierarchyContext.createForDocumentsField(fields).getNumEmbeddedDocumentsLayers())
        .isEqualTo(3);

    assertWithMessage("should be max 4 layers deep")
        .that(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getNumEmbeddedDocumentsLayers())
        .isEqualTo(4);
  }

  @Test
  public void testRelativeRoots() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = Map.of("foo", embeddedAndDocument());

    assertThat(
            FieldHierarchyContext.createForDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(ImmutableSet.of(FieldPath.parse("foo")));

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(ImmutableSet.of(FieldPath.parse("foo")));
  }

  @Test
  public void testGetRootFields() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = Map.of("foo", embeddedAndDocument());

    ImmutableMap<FieldPath, FieldDefinition> expected =
        ImmutableMap.of(FieldPath.parse("foo"), fields.get("foo"));

    assertThat(FieldHierarchyContext.createForDocumentsField(fields).getRootFields())
        .isEqualTo(expected);

    assertThat(FieldHierarchyContext.createForEmbeddedDocumentsField(fields).getRootFields())
        .isEqualTo(expected);
  }

  @Test
  public void testFieldsByEmbeddedRoot() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = Map.of("foo", embeddedAndDocument());

    ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>> expected =
        ImmutableMap.of(FieldPath.parse("foo"), ImmutableMap.of());

    assertThat(FieldHierarchyContext.createForDocumentsField(fields).getFieldsByEmbeddedRoot())
        .isEqualTo(expected);

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields).getFieldsByEmbeddedRoot())
        .isEqualTo(expected);
  }

  @Test
  public void testEmbeddedFieldDefinitionByEmbeddedRoot() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = Map.of("foo", embeddedAndDocument());

    ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition> expected =
        ImmutableMap.of(
            FieldPath.parse("foo"), EmbeddedDocumentsFieldDefinitionBuilder.builder().build());

    assertThat(
            FieldHierarchyContext.createForDocumentsField(fields)
                .getEmbeddedFieldDefinitionByEmbeddedRoot())
        .isEqualTo(expected);

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getEmbeddedFieldDefinitionByEmbeddedRoot())
        .isEqualTo(expected);
  }

  @Test
  public void testMultipleRelativeRootsSameLevel() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("foo", embeddedAndDocument()),
            Map.entry("bar", embeddedAndDocument()),
            Map.entry("baz", embeddedAndDocument()));

    ImmutableSet<FieldPath> expected =
        ImmutableSet.of(FieldPath.parse("foo"), FieldPath.parse("bar"), FieldPath.parse("baz"));

    assertThat(
            FieldHierarchyContext.createForDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(expected);

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(expected);
  }

  @Test
  public void testGetRootFieldsMultipleRootsSameLevel() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("foo", embeddedAndDocument()),
            Map.entry("bar", embeddedAndDocument()),
            Map.entry("baz", embeddedAndDocument()));

    ImmutableMap<FieldPath, FieldDefinition> expected =
        ImmutableMap.of(
            FieldPath.parse("foo"), fields.get("foo"),
            FieldPath.parse("bar"), fields.get("bar"),
            FieldPath.parse("baz"), fields.get("baz"));

    assertThat(FieldHierarchyContext.createForDocumentsField(fields).getRootFields())
        .isEqualTo(expected);

    assertThat(FieldHierarchyContext.createForEmbeddedDocumentsField(fields).getRootFields())
        .isEqualTo(expected);
  }

  @Test
  public void testFieldsByEmbeddedRootMultipleRootsSameLevel()
      throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("foo", embeddedAndDocument()),
            Map.entry("bar", embeddedAndDocument()),
            Map.entry("baz", embeddedAndDocument()));

    ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>> expected =
        ImmutableMap.of(
            FieldPath.parse("foo"), ImmutableMap.of(),
            FieldPath.parse("bar"), ImmutableMap.of(),
            FieldPath.parse("baz"), ImmutableMap.of());

    assertThat(FieldHierarchyContext.createForDocumentsField(fields).getFieldsByEmbeddedRoot())
        .isEqualTo(expected);

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields).getFieldsByEmbeddedRoot())
        .isEqualTo(expected);
  }

  @Test
  public void testDescendentRootsFromDifferentBranches() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = complexMappings();

    assertThat(
            FieldHierarchyContext.createForDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(
            Set.of(
                FieldPath.parse("a"),
                FieldPath.parse("a.b"),
                FieldPath.parse("a.b.c"),
                FieldPath.parse("a.b.c.d"),
                FieldPath.parse("a.b.c.d.e"),
                FieldPath.parse("a.b.c.d.e.f")));

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(
            Set.of(
                FieldPath.parse("a"),
                FieldPath.parse("a.b"),
                FieldPath.parse("a.b.c"),
                FieldPath.parse("a.b.c.d"),
                FieldPath.parse("a.b.c.d.e"),
                FieldPath.parse("a.b.c.d.e.f")));
  }

  @Test
  public void testGetRootFieldsDescendantFromDifferentBranches()
      throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = complexMappings();

    for (var rootFields :
        List.of(
            FieldHierarchyContext.createForDocumentsField(fields).getRootFields(),
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields).getRootFields())) {

      assertThat(rootFields).containsKey(FieldPath.parse("a"));
      assertThat(rootFields).containsKey(FieldPath.parse("a.b"));
      assertThat(rootFields).containsKey(FieldPath.parse("a.b.c"));

      assertThat(rootFields.get(FieldPath.parse("a")).documentFieldDefinition()).isPresent();
      assertThat(rootFields.get(FieldPath.parse("a")).embeddedDocumentsFieldDefinition())
          .isPresent();

      assertThat(rootFields.get(FieldPath.parse("a.b")).documentFieldDefinition()).isPresent();
      assertThat(rootFields.get(FieldPath.parse("a.b")).embeddedDocumentsFieldDefinition())
          .isPresent();

      assertThat(rootFields.get(FieldPath.parse("a.b.c")).documentFieldDefinition()).isEmpty();
      assertThat(rootFields.get(FieldPath.parse("a.b.c")).embeddedDocumentsFieldDefinition())
          .isPresent();
    }
  }

  @Test
  public void testFieldsByEmbeddedRootDescendantFromDifferentBranches()
      throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields = complexMappings();

    for (var fieldHierarchyContext :
        List.of(
            FieldHierarchyContext.createForDocumentsField(fields),
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields))) {

      var fieldsByEmbeddedRoot = fieldHierarchyContext.getFieldsByEmbeddedRoot();
      var embeddedFieldDefinitionByEmbeddedRoot =
          fieldHierarchyContext.getEmbeddedFieldDefinitionByEmbeddedRoot();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a"));
      var embeddedAFields = fieldsByEmbeddedRoot.get(FieldPath.parse("a"));
      assertWithMessage("5 fields defined in embedded doc at a")
          .that(embeddedAFields.size())
          .isEqualTo(5);

      assertThat(embeddedAFields.get(FieldPath.parse("a.b")).hasScalarFieldDefinitions()).isFalse();
      assertThat(embeddedAFields.get(FieldPath.parse("a.b.c")).hasScalarFieldDefinitions())
          .isFalse();
      assertThat(embeddedAFields.get(FieldPath.parse("a.b.c.d")).hasScalarFieldDefinitions())
          .isFalse();
      assertThat(embeddedAFields.get(FieldPath.parse("a.b.c.d.e")).hasScalarFieldDefinitions())
          .isFalse();
      assertWithMessage("has embedded field definition at a.b.c.d.e.f")
          .that(embeddedAFields.get(FieldPath.parse("a.b.c.d.e.f")).hasScalarFieldDefinitions())
          .isTrue();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a.b"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a.b"));
      var embeddedAbFields = fieldsByEmbeddedRoot.get(FieldPath.parse("a.b"));
      assertWithMessage("2 fields defined in embedded doc at a.b")
          .that(embeddedAbFields.size())
          .isEqualTo(2);
      assertThat(embeddedAbFields).containsKey(FieldPath.parse("a.b.c"));
      assertThat(embeddedAbFields.get(FieldPath.parse("a.b.c")).hasScalarFieldDefinitions())
          .isFalse();
      assertThat(embeddedAbFields).containsKey(FieldPath.parse("a.b.c.d"));
      assertWithMessage("has embedded field def at a.b.c.d")
          .that(embeddedAbFields.get(FieldPath.parse("a.b.c.d")).hasScalarFieldDefinitions())
          .isTrue();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c"));
      var embeddedAbcFields = fieldsByEmbeddedRoot.get(FieldPath.parse("a.b.c"));
      assertWithMessage("2 fields defined in embedded doc at a.b.c")
          .that(embeddedAbcFields.size())
          .isEqualTo(2);
      assertThat(embeddedAbcFields).containsKey(FieldPath.parse("a.b.c.d"));
      assertThat(embeddedAbcFields.get(FieldPath.parse("a.b.c.d")).hasScalarFieldDefinitions())
          .isFalse();
      assertThat(embeddedAbcFields).containsKey(FieldPath.parse("a.b.c.d.e"));
      assertWithMessage("has embedded field def at a.b.c.d.e")
          .that(embeddedAbcFields.get(FieldPath.parse("a.b.c.d.e")).hasScalarFieldDefinitions())
          .isTrue();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d"));
      var embeddedAbcdFields = fieldsByEmbeddedRoot.get(FieldPath.parse("a.b.c.d"));
      assertWithMessage("2 fields defined in embedded doc at a.b.c.d")
          .that(embeddedAbcdFields.size())
          .isEqualTo(1);
      assertThat(embeddedAbcdFields).containsKey(FieldPath.parse("a.b.c.d.e"));
      assertThat(embeddedAbcdFields.get(FieldPath.parse("a.b.c.d.e")).hasScalarFieldDefinitions())
          .isFalse();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d.e"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d.e"));
      assertThat(fieldsByEmbeddedRoot.get(FieldPath.parse("a.b.c.d.e"))).isEmpty();

      assertThat(fieldsByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d.e.f"));
      assertThat(embeddedFieldDefinitionByEmbeddedRoot).containsKey(FieldPath.parse("a.b.c.d.e.f"));
      assertThat(fieldsByEmbeddedRoot.get(FieldPath.parse("a.b.c.d.e.f"))).isEmpty();
    }
  }

  @Test
  public void testRelativeRootsDifferentBranches() throws IllegalEmbeddedFieldException {
    Map<String, FieldDefinition> fields =
        Map.ofEntries(
            Map.entry("a", document("b", embedded("c", document("d", embedded())))),
            Map.entry("g", document("h", document("i", embedded("j", embedded("l", document()))))),
            Map.entry("m", embedded("n", embedded("o", embeddedAndDocument()))));

    assertThat(
            FieldHierarchyContext.createForDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(
            Set.of(
                FieldPath.parse("a.b"),
                FieldPath.parse("a.b.c.d"),
                FieldPath.parse("g.h.i"),
                FieldPath.parse("g.h.i.j"),
                FieldPath.parse("m"),
                FieldPath.parse("m.n"),
                FieldPath.parse("m.n.o")));

    assertThat(
            FieldHierarchyContext.createForEmbeddedDocumentsField(fields)
                .getEmbeddedDocumentsRelativeRoots())
        .isEqualTo(
            Set.of(
                FieldPath.parse("a.b"),
                FieldPath.parse("a.b.c.d"),
                FieldPath.parse("g.h.i"),
                FieldPath.parse("g.h.i.j"),
                FieldPath.parse("m"),
                FieldPath.parse("m.n"),
                FieldPath.parse("m.n.o")));
  }

  @Test
  public void testThrowsOnShallowRootClash() {
    Map<String, FieldDefinition> fields =
        Map.of(
            "a",
            FieldDefinitionBuilder.builder()
                .document(DocumentFieldDefinitionBuilder.builder().field("b", embedded()).build())
                .embeddedDocuments(
                    EmbeddedDocumentsFieldDefinitionBuilder.builder()
                        .field("b", embedded())
                        .build())
                .build());

    try {
      FieldHierarchyContext.createForDocumentsField(fields);
    } catch (IllegalEmbeddedFieldException exception) {
      assertThat(exception.getMessage())
          .isEqualTo("cannot define multiple embeddedDocuments fields at sub-paths a.b");
      return;
    }
    assertWithMessage("should throw when multiple embeddedDocuments fields share the same path")
        .fail();
  }

  /*
   {
     "mappings": {
       "fields": {
         "a": [
           {
             "type": "document",
             "fields": {
               "b": [
                 {
                   "type": "document",
                   "fields": {
                     "c": {
                       "type": "embeddedDocuments",
                       "fields": {
                         "d": {
                           "type": "document",
                           "fields": {
                             "e": {
                               "type": "embeddedDocuments"
                             }
                           }
                         }
                       }
                     }
                   }
                 },
                 {
                   "type": "embeddedDocuments",
                   "fields": {
                     "c": {
                       "type": "document",
                       "fields": {
                         "d": {
                           "type": "embeddedDocuments",
                           "fields": {
                             "e": {
                               "type": "document"
                             }
                           }
                         }
                       }
                     }
                   }
                 }
               ]
             }
           },
           {
             "type": "embeddedDocuments",
             "fields": {
               "b": {
                 "type": "document",
                 "fields": {
                   "c": {
                     "type": "document",
                     "fields": {
                       "d": {
                         "type": "document",
                         "fields": {
                           "e": {
                             "type": "embeddedDocuments",
                             "fields": {
                               "f": {
                                 "type": "embeddedDocuments"
                               }
                             }
                           }
                         }
                       }
                     }
                   }
                 }
               }
             }
           }
         ]
       }
     }
   }
  */
  @Test
  public void testThrowsOnDeepRootClash() {
    Map<String, FieldDefinition> fields =
        Map.of(
            "a",
            FieldDefinitionBuilder.builder()
                .document(
                    DocumentFieldDefinitionBuilder.builder()
                        .field(
                            "b",
                            FieldDefinitionBuilder.builder()
                                .document(
                                    DocumentFieldDefinitionBuilder.builder()
                                        .field("c", embedded("d", document("e", embedded())))
                                        .build())
                                .embeddedDocuments(
                                    EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                        .field("c", document("d", embedded("e", document())))
                                        .build())
                                .build())
                        .build())
                .embeddedDocuments(
                    EmbeddedDocumentsFieldDefinitionBuilder.builder()
                        .field(
                            "b",
                            document("c", document("d", document("e", embedded("f", embedded())))))
                        .build())
                .build());

    try {
      FieldHierarchyContext.createForDocumentsField(fields);
    } catch (IllegalEmbeddedFieldException exception) {
      assertThat(exception.getMessage())
          .isEqualTo("cannot define multiple embeddedDocuments fields at sub-paths a.b.c.d.e");
      return;
    }
    assertWithMessage("should throw when multiple embeddedDocuments fields share the same path")
        .fail();
  }

  @Test
  public void testEquals() {
    TestUtils.assertEqualityGroups(
        () -> new FieldHierarchyContext(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), 0),
        () -> new FieldHierarchyContext(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), 1),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(FieldPath.parse("a"), embedded()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                0),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(FieldPath.parse("a"), document()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                0),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(FieldPath.parse("b"), embedded()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                0),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(),
                ImmutableMap.of(
                    FieldPath.parse("a"), ImmutableMap.of(FieldPath.parse("a"), embedded())),
                ImmutableMap.of(),
                0),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(),
                ImmutableMap.of(
                    FieldPath.parse("a"), ImmutableMap.of(FieldPath.parse("a"), document())),
                ImmutableMap.of(),
                0),
        () ->
            new FieldHierarchyContext(
                ImmutableMap.of(),
                ImmutableMap.of(
                    FieldPath.parse("b"), ImmutableMap.of(FieldPath.parse("b"), embedded())),
                ImmutableMap.of(),
                0));
  }

  /*
   {
     "mappings": {
       "fields": {
         "a": [
           {
             "type": "document",
             "fields": {
               "b": [
                 {
                   "type": "document",
                   "fields": {
                     "c": {
                       "type": "embeddedDocuments",
                       "fields": {
                         "d": {
                           "type": "document",
                           "fields": {
                             "e": {
                               "type": "embeddedDocuments"
                             }
                           }
                         }
                       }
                     }
                   }
                 },
                 {
                   "type": "embeddedDocuments",
                   "fields": {
                     "c": {
                       "type": "document",
                       "fields": {
                         "d": {
                           "type": "embeddedDocuments",
                           "fields": {
                             "e": {
                               "type": "document"
                             }
                           }
                         }
                       }
                     }
                   }
                 }
               ]
             }
           },
           {
             "type": "embeddedDocuments",
             "fields": {
               "b": {
                 "type": "document",
                 "fields": {
                   "c": {
                     "type": "document",
                     "fields": {
                       "d": {
                         "type": "document",
                         "fields": {
                           "e": {
                             "type": "document",
                             "fields": {
                               "f": {
                                 "type": "embeddedDocuments"
                               }
                             }
                           }
                         }
                       }
                     }
                   }
                 }
               }
             }
           }
         ]
       }
     }
   }
  */
  private static Map<String, FieldDefinition> complexMappings() {
    return Map.of(
        "a",
        FieldDefinitionBuilder.builder()
            .document(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "b",
                        FieldDefinitionBuilder.builder()
                            .document(
                                DocumentFieldDefinitionBuilder.builder()
                                    .field("c", embedded("d", document("e", embedded())))
                                    .build())
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .field("c", document("d", embedded("e", document())))
                                    .build())
                            .build())
                    .build())
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .field(
                        "b", document("c", document("d", document("e", document("f", embedded())))))
                    .build())
            .build());
  }

  static FieldDefinition embedded() {
    return FieldDefinitionBuilder.builder()
        .embeddedDocuments(EmbeddedDocumentsFieldDefinitionBuilder.builder().build())
        .build();
  }

  static FieldDefinition embedded(String path, FieldDefinition field) {
    return FieldDefinitionBuilder.builder()
        .embeddedDocuments(
            EmbeddedDocumentsFieldDefinitionBuilder.builder().field(path, field).build())
        .build();
  }

  static FieldDefinition document() {
    return FieldDefinitionBuilder.builder()
        .document(DocumentFieldDefinitionBuilder.builder().build())
        .build();
  }

  static FieldDefinition document(String path, FieldDefinition field) {
    return FieldDefinitionBuilder.builder()
        .document(DocumentFieldDefinitionBuilder.builder().field(path, field).build())
        .build();
  }

  static FieldDefinition embeddedAndDocument() {
    return FieldDefinitionBuilder.builder()
        .embeddedDocuments(EmbeddedDocumentsFieldDefinitionBuilder.builder().build())
        .document(DocumentFieldDefinitionBuilder.builder().build())
        .build();
  }
}
