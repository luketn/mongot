package com.xgen.testing.mongot.integration.index.serialization;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.bson.BsonDocument;

public class VectorIndexSpec extends IndexSpec {

  public static final VectorIndexSpec EMPTY =
      new VectorIndexSpec(
          ImmutableList.of(),
          IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
          Fields.RUN_FOR.getDefaultValue(),
          Optional.of(StoredSourceDefinition.defaultValue()));

  static class Fields {
    static final Field.Required<List<VectorIndexFieldDefinition>> FIELDS =
        Field.builder("fields")
            .classField(VectorIndexFieldDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();

    static final Field.WithDefault<VectorIndexRunForSpec> RUN_FOR =
        Field.builder("runFor")
            .classField(VectorIndexRunForSpec::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(VectorIndexRunForSpec.DEFAULT);

    static final Field.Optional<StoredSourceDefinition> STORED_SOURCE =
        Field.builder("storedSource")
            .classField(StoredSourceDefinition::fromBson)
            .optional()
            .noDefault();
  }

  private final List<VectorIndexFieldDefinition> fields;
  private final int numPartitions;
  private final VectorIndexRunForSpec runFor;
  private final Optional<StoredSourceDefinition> storedSource;

  public VectorIndexSpec(
      List<VectorIndexFieldDefinition> fields,
      int numPartitions,
      VectorIndexRunForSpec runFor,
      Optional<StoredSourceDefinition> storedSource) {
    this.fields = fields;
    this.numPartitions = numPartitions;
    this.runFor = runFor;
    this.storedSource = storedSource;
  }

  @Override
  public Type getType() {
    return Type.VECTOR_SEARCH;
  }

  @Override
  public List<IndexFormatVersion> getIndexFormatVersions() {
    int from = this.runFor.getIndexFormatVersion().getFrom();
    int to = this.runFor.getIndexFormatVersion().getTo();
    return IntStream.rangeClosed(from, to).mapToObj(IndexFormatVersion::create).toList();
  }

  @Override
  public int getNumPartitions() {
    return this.numPartitions;
  }

  @Override
  public List<Integer> getIndexFeatureVersions() {
    int from = this.runFor.getIndexFeatureVersion().getFrom();
    int to = this.runFor.getIndexFeatureVersion().getTo();
    return IntStream.rangeClosed(from, to).boxed().toList();
  }

  public VectorIndexRunForSpec getRunFor() {
    return this.runFor;
  }

  public Optional<StoredSourceDefinition> getStoredSource() {
    return this.storedSource;
  }

  /** Builds IndexSpec from Bson. */
  public static VectorIndexSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new VectorIndexSpec(
        parser.getField(Fields.FIELDS).unwrap(),
        parser.getField(IndexDefinition.Fields.NUM_PARTITIONS).unwrap(),
        parser.getField(Fields.RUN_FOR).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder();
    builder.field(Fields.FIELDS, this.fields);
    builder.field(Fields.RUN_FOR, this.runFor);
    this.storedSource.ifPresent(
        storedSource -> builder.field(Fields.STORED_SOURCE, Optional.of(storedSource)));
    return builder.build();
  }

  public List<VectorIndexFieldDefinition> getFields() {
    return this.fields;
  }
}
