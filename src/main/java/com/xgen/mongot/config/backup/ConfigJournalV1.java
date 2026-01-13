package com.xgen.mongot.config.backup;

import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * See the config journals section in: src/main/java/com/xgen/mongot/config/README.md
 *
 * <p>A config journal describing indexes in different generations and states. One index generation
 * can be either staged, live or deleted.
 *
 * <ul>
 *   <li>Staged - Means that the index is staged to swap a live index once it is ready.
 *   <li>Live - Index should be queryable.
 *   <li>Deleted - Index that should be deleted once this journal is used to restore mongot.
 * </ul>
 */
public final class ConfigJournalV1 implements DocumentEncodable {
  public static final int VERSION = 1;

  public static class Fields {
    static final Field.Required<Integer> VERSION_FIELD =
        Field.builder("version")
            .intField()
            .validate(
                x ->
                    x == VERSION
                        ? Optional.empty()
                        : Optional.of(
                            String.format("must be set to %s for this config format.", VERSION)))
            .required();

    static final Field.Required<List<SearchIndexDefinitionGeneration>> STAGED_SEARCH_INDEXES =
        searchDefinitionsField("stagedIndexes");
    static final Field.Required<List<SearchIndexDefinitionGeneration>> LIVE_SEARCH_INDEXES =
        searchDefinitionsField("indexes");
    static final Field.Required<List<SearchIndexDefinitionGeneration>> DELETED_SEARCH_INDEXES =
        searchDefinitionsField("deletedIndexes");

    static final Field.WithDefault<List<VectorIndexDefinitionGeneration>> STAGED_VECTOR_INDEXES =
        vectorDefinitionsField("stagedVectorIndexes");
    static final Field.WithDefault<List<VectorIndexDefinitionGeneration>> LIVE_VECTOR_INDEXES =
        vectorDefinitionsField("vectorIndexes");
    static final Field.WithDefault<List<VectorIndexDefinitionGeneration>> DELETED_VECTOR_INDEXES =
        vectorDefinitionsField("deletedVectorIndexes");

    private static Field.Required<List<SearchIndexDefinitionGeneration>> searchDefinitionsField(
        String name) {
      return Field.builder(name)
          .classField(SearchIndexDefinitionGeneration::fromBson)
          .disallowUnknownFields()
          .asList()
          .required();
    }

    private static Field.WithDefault<List<VectorIndexDefinitionGeneration>> vectorDefinitionsField(
        String name) {
      return Field.builder(name)
          .classField(VectorIndexDefinitionGeneration::fromBson)
          .disallowUnknownFields()
          .asList()
          .optional()
          .withDefault(List.of());
    }
  }

  /** Compares IndexDefinitionGeneration by their generation id. */
  private static final Comparator<IndexDefinitionGeneration> DEFINITION_SORTER =
      Comparator.comparing(
          IndexDefinitionGeneration::getGenerationId,
          /* First we compare indexIds, then break ties by format and user versions.*/
          Comparator.<GenerationId, ObjectId>comparing(genId1 -> genId1.indexId)
              .thenComparingInt(genId -> genId.generation.indexFormatVersion.versionNumber)
              .thenComparingInt(genId -> genId.generation.userIndexVersion.versionNumber));

  private final ImmutableList<SearchIndexDefinitionGeneration> stagedSearchIndexes;
  private final ImmutableList<SearchIndexDefinitionGeneration> liveSearchIndexes;
  private final ImmutableList<SearchIndexDefinitionGeneration> deletedSerchIndexes;

  private final ImmutableList<VectorIndexDefinitionGeneration> stagedVectorIndexes;
  private final ImmutableList<VectorIndexDefinitionGeneration> liveVectorIndexes;
  private final ImmutableList<VectorIndexDefinitionGeneration> deletedVectorIndexes;

  public ConfigJournalV1(
      List<SearchIndexDefinitionGeneration> stagedSearchIndexes,
      List<SearchIndexDefinitionGeneration> liveSearchIndexes,
      List<SearchIndexDefinitionGeneration> deletedSearchIndexes,
      List<VectorIndexDefinitionGeneration> stagedVectorIndexes,
      List<VectorIndexDefinitionGeneration> liveVectorIndexes,
      List<VectorIndexDefinitionGeneration> deletedVectorIndexes) {
    this.stagedSearchIndexes = consistentOrdering(stagedSearchIndexes);
    this.liveSearchIndexes = consistentOrdering(liveSearchIndexes);
    this.deletedSerchIndexes = consistentOrdering(deletedSearchIndexes);
    this.stagedVectorIndexes = consistentOrdering(stagedVectorIndexes);
    this.liveVectorIndexes = consistentOrdering(liveVectorIndexes);
    this.deletedVectorIndexes = consistentOrdering(deletedVectorIndexes);
  }

  public ConfigJournalV1(
      List<IndexDefinitionGeneration> staged,
      List<IndexDefinitionGeneration> live,
      List<IndexDefinitionGeneration> deleted) {
    this.stagedSearchIndexes = consistentOrdering(extractSearchIndexDefinitionGeneration(staged));
    this.liveSearchIndexes = consistentOrdering(extractSearchIndexDefinitionGeneration(live));
    this.deletedSerchIndexes = consistentOrdering(extractSearchIndexDefinitionGeneration(deleted));
    this.stagedVectorIndexes = consistentOrdering(extractVectorIndexDefinitionGeneration(staged));
    this.liveVectorIndexes = consistentOrdering(extractVectorIndexDefinitionGeneration(live));
    this.deletedVectorIndexes = consistentOrdering(extractVectorIndexDefinitionGeneration(deleted));
  }

  private static List<SearchIndexDefinitionGeneration> extractSearchIndexDefinitionGeneration(
      List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .filter(d -> d.getType() == Type.SEARCH)
        .map(IndexDefinitionGeneration::asSearch)
        .collect(Collectors.toList());
  }

  private static List<VectorIndexDefinitionGeneration> extractVectorIndexDefinitionGeneration(
      List<IndexDefinitionGeneration> definitions) {
    return definitions.stream()
        .filter(d -> d.getType() == Type.VECTOR)
        .map(IndexDefinitionGeneration::asVector)
        .collect(Collectors.toList());
  }

  public static Optional<ConfigJournalV1> fromFileIfExists(Path path)
      throws IOException, BsonParseException {
    File file = path.toFile();
    if (!file.exists()) {
      return Optional.empty();
    }

    String json = Files.readString(path);
    BsonDocument document = JsonCodec.fromJson(json);

    try (BsonDocumentParser parser =
        BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return Optional.of(ConfigJournalV1.fromBson(parser));
    }
  }

  /** deserialize, throws if the document represents a config format from a different version. */
  public static ConfigJournalV1 fromBson(DocumentParser parser) throws BsonParseException {
    // make sure this is the correct version:
    parser.getField(Fields.VERSION_FIELD).unwrap();

    return new ConfigJournalV1(
        parser.getField(Fields.STAGED_SEARCH_INDEXES).unwrap(),
        parser.getField(Fields.LIVE_SEARCH_INDEXES).unwrap(),
        parser.getField(Fields.DELETED_SEARCH_INDEXES).unwrap(),
        parser.getField(Fields.STAGED_VECTOR_INDEXES).unwrap(),
        parser.getField(Fields.LIVE_VECTOR_INDEXES).unwrap(),
        parser.getField(Fields.DELETED_VECTOR_INDEXES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VERSION_FIELD, VERSION)
        .field(Fields.STAGED_SEARCH_INDEXES, this.stagedSearchIndexes)
        .field(Fields.LIVE_SEARCH_INDEXES, this.liveSearchIndexes)
        .field(Fields.DELETED_SEARCH_INDEXES, this.deletedSerchIndexes)
        .field(Fields.STAGED_VECTOR_INDEXES, this.stagedVectorIndexes)
        .field(Fields.LIVE_VECTOR_INDEXES, this.liveVectorIndexes)
        .field(Fields.DELETED_VECTOR_INDEXES, this.deletedVectorIndexes)
        .build();
  }

  public ImmutableList<IndexDefinitionGeneration> getStagedIndexes() {
    return ImmutableList.copyOf(
        Iterables.concat(this.stagedSearchIndexes, this.stagedVectorIndexes));
  }

  public ImmutableList<IndexDefinitionGeneration> getLiveIndexes() {
    return ImmutableList.copyOf(Iterables.concat(this.liveSearchIndexes, this.liveVectorIndexes));
  }

  public ImmutableList<IndexDefinitionGeneration> getDeletedIndexes() {
    return ImmutableList.copyOf(
        Iterables.concat(this.deletedSerchIndexes, this.deletedVectorIndexes));
  }

  private static <T extends IndexDefinitionGeneration> ImmutableList<T> consistentOrdering(
      List<T> definitions) {
    // To keep a consistent ordering of definitions in our config, sort the definitions by
    // generation ids.
    // We do not rely on this order in any way, however it is convenient for reading and testing.
    return definitions.stream().sorted(DEFINITION_SORTER).collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigJournalV1 that = (ConfigJournalV1) o;
    return Objects.equal(this.stagedSearchIndexes, that.stagedSearchIndexes)
        && Objects.equal(this.liveSearchIndexes, that.liveSearchIndexes)
        && Objects.equal(this.deletedSerchIndexes, that.deletedSerchIndexes)
        && Objects.equal(this.stagedVectorIndexes, that.stagedVectorIndexes)
        && Objects.equal(this.liveVectorIndexes, that.liveVectorIndexes)
        && Objects.equal(this.deletedVectorIndexes, that.deletedVectorIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.stagedSearchIndexes,
        this.liveSearchIndexes,
        this.deletedSerchIndexes,
        this.stagedVectorIndexes,
        this.liveVectorIndexes,
        this.deletedVectorIndexes);
  }
}
