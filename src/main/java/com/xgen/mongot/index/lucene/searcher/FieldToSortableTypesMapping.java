package com.xgen.mongot.index.lucene.searcher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Sets;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;

public record FieldToSortableTypesMapping(
    ImmutableSetMultimap<FieldPath, FieldName.TypeField> rootFieldsToSortableTypes,
    ImmutableMap<FieldPath, ImmutableSetMultimap<FieldPath, FieldName.TypeField>>
        embeddedFieldsToSortableTypes) {
  private static final ImmutableSet<FieldName.TypeField> SORTABLE_FIELD_TYPES =
      Sets.immutableEnumSet(
          FieldName.TypeField.BOOLEAN,
          FieldName.TypeField.DATE_V2,
          FieldName.TypeField.NUMBER_INT64_V2,
          FieldName.TypeField.NUMBER_DOUBLE_V2,
          FieldName.TypeField.TOKEN,
          FieldName.TypeField.UUID,
          FieldName.TypeField.NULL,
          FieldName.TypeField.OBJECT_ID,
          FieldName.TypeField.SORTABLE_STRING_BETA_V1,
          FieldName.TypeField.SORTABLE_NUMBER_BETA_V1,
          FieldName.TypeField.SORTABLE_DATE_BETA_V1);

  public static FieldToSortableTypesMapping create(IndexReader reader) {
    var embeddedAndRootFields =
        StreamSupport.stream(FieldInfos.getMergedFieldInfos(reader).spliterator(), false)
            .filter(FieldToSortableTypesMapping::isSortableType)
            .collect(
                Collectors.partitioningBy(
                    fieldInfo -> FieldName.EmbeddedField.isTypeOf(fieldInfo.name)));

    var rootFieldsToSortableTypes =
        embeddedAndRootFields.getOrDefault(false, List.of()).stream()
            .collect(
                ImmutableSetMultimap.toImmutableSetMultimap(
                    FieldToSortableTypesMapping::getField,
                    fieldInfo -> FieldName.TypeField.getTypeOf(fieldInfo.name).get()));

    var embeddedFieldsToSortableTypes =
        embeddedAndRootFields.getOrDefault(true, List.of()).stream()
            .collect(
                Collectors.collectingAndThen(
                    Collectors.groupingBy(
                        FieldToSortableTypesMapping::getEmbeddedRoot,
                        ImmutableSetMultimap.toImmutableSetMultimap(
                            FieldToSortableTypesMapping::getField,
                            fieldInfo -> FieldName.TypeField.getTypeOf(fieldInfo.name).get())),
                    ImmutableMap::copyOf));

    return new FieldToSortableTypesMapping(
        rootFieldsToSortableTypes, embeddedFieldsToSortableTypes);
  }

  private static boolean isSortableType(FieldInfo fieldInfo) {
    Optional<FieldName.TypeField> maybeTypeField = FieldName.TypeField.getTypeOf(fieldInfo.name);
    return maybeTypeField.isPresent() && SORTABLE_FIELD_TYPES.contains(maybeTypeField.get());
  }

  private static FieldPath getField(FieldInfo info) {
    return FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(info.name));
  }

  private static FieldPath getEmbeddedRoot(FieldInfo info) {
    return FieldPath.parse(FieldName.getEmbeddedRootPathOrThrow(info.name));
  }

  public ImmutableSetMultimap<FieldPath, FieldName.TypeField> getFieldToSortableTypes(
      Optional<FieldPath> embeddedRoot) {
    return embeddedRoot
        .map(
            path ->
                this.embeddedFieldsToSortableTypes.getOrDefault(path, ImmutableSetMultimap.of()))
        .orElse(this.rootFieldsToSortableTypes);
  }
}
