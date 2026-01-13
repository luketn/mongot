package com.xgen.mongot.index.definition;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

public class IndexSortValidator {
  private static Trie<String, FieldDefinition> buildStaticFieldsIndex(
      DocumentFieldDefinition mappings) {
    Trie<String, FieldDefinition> fields = new PatriciaTrie<>();
    SearchIndexDefinition.registerFields(fields, Optional.empty(), mappings);
    return fields;
  }

  static void validateNoScoreFields(Sort sort) throws BsonParseException {
    Set<String> scoreFields = sort.getSortFields().stream()
        .filter(f -> f.options() instanceof MetaSortOptions)
        .map(f -> f.field().toString())
        .collect(Collectors.toSet());

    if (!scoreFields.isEmpty()) {
      throw new BsonParseException(
          String.format("Cannot sort on score for fields: %s", scoreFields),
          Optional.empty());
    }
  }

  static void validateSortFieldsStaticallyDefined(
      Sort sort,
      Trie<String, FieldDefinition> staticFields) throws BsonParseException {
    Set<String> undefinedSortFields = sort.getSortFields().stream()
        .map(f -> f.field().toString())
        .filter(f -> !staticFields.containsKey(f))
        .collect(Collectors.toSet());

    if (!undefinedSortFields.isEmpty()) {
      throw new BsonParseException(
          String.format("Sort fields: %s are not statically defined", undefinedSortFields),
          Optional.empty());
    }
  }

  // TODO(CLOUDP-356691): support multitype sort fields.
  static void validateSortFieldsHaveSingleType(
      Sort sort,
      Trie<String, FieldDefinition> staticFields) throws BsonParseException {
    Set<String> multiTypeOrEmptySortFields = new HashSet<>();
    for (MongotSortField sortField : sort.getSortFields()) {
      FieldDefinition fieldDefinition =
          Check.isNotNull(staticFields.get(sortField.field().toString()), "fieldDefinition");
      List<? extends FieldTypeDefinition> allDefinitions =
          fieldDefinition.getAllDefinitions().flatMap(Optional::stream).toList();
      if (allDefinitions.size() != 1) {
        multiTypeOrEmptySortFields.add(sortField.field().toString());
      }
    }

    if (!multiTypeOrEmptySortFields.isEmpty()) {
      throw new BsonParseException(
          String.format("Sort fields: %s have mixed types or are not defined",
              multiTypeOrEmptySortFields),
          Optional.empty());
    }
  }

  static void validateSortFieldsAreSortable(
      Sort sort,
      Trie<String, FieldDefinition> staticFields) throws BsonParseException {
    Map<String, FieldTypeDefinition> sortFieldDefinitions =
        sort.getSortFields().stream()
            .collect(
                Collectors.toMap(
                    f -> f.field().toString(),
                    f -> {
                      FieldDefinition fieldDefinition =
                          Check.isNotNull(
                              staticFields.get(f.field().toString()), "fieldDefinition");
                      // Since validateSortFieldsAreSortable is called after
                      // validateSortFieldsHaveSingleType there has to be exactly one definition.
                      return Check.isNotNull(fieldDefinition
                          .getAllDefinitions()
                          .flatMap(Optional::stream)
                          .toList()
                          .getFirst(), "fieldDefinition");
                    },
                    (a, b) -> b
                )
            );
    Map<String, FieldTypeDefinition.Type> nonSortableTypes = new HashMap<>();
    for (var entry : sortFieldDefinitions.entrySet()) {
      if (!FieldDefinition.INDEXING_SORTABLE_TYPES.contains(entry.getValue().getType())) {
        nonSortableTypes.put(entry.getKey(), entry.getValue().getType());
      }
    }
    if (!nonSortableTypes.isEmpty()) {
      throw new BsonParseException(
          String.format(
              "Sort fields: %s are not sortable types. Sortable types are: %s",
              nonSortableTypes, FieldDefinition.INDEXING_SORTABLE_TYPES),
          Optional.empty());
    }
  }

  public static void checkSortedIndexEnabled(
      SearchIndexDefinition index, FeatureFlags featureFlags) {
    if (index.getSort().isPresent() && !featureFlags.isEnabled(Feature.SORTED_INDEX)) {
      throw new IllegalArgumentException("Sort configuration is not supported, "
          + "Feature flag SORTED_INDEX is not enabled.");
    }
  }

  /**
   * Validates that the sort configuration is compatible with the field mappings.
   *
   * @param optionalSort the sort configuration to validate
   * @param mappings the document field mappings
   * @throws BsonParseException if validation fails
   */
  public static void validateSort(
      Optional<Sort> optionalSort,
      DocumentFieldDefinition mappings) throws BsonParseException {
    if (optionalSort.isEmpty()) {
      return;
    }

    Trie<String, FieldDefinition> staticFields = buildStaticFieldsIndex(mappings);
    Sort sort = optionalSort.get();

    validateNoScoreFields(sort);
    validateSortFieldsStaticallyDefined(sort, staticFields);
    validateSortFieldsHaveSingleType(sort, staticFields);
    validateSortFieldsAreSortable(sort, staticFields);
  }

}
