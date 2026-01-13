package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.VIEW;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents a MongoDB view. In case the view is based on another view(s), {@link
 * #effectivePipeline} is a concatenation of the parent view pipelines.
 */
public class ViewDefinition implements Encodable {

  public static class Fields {
    public static final Field.Required<String> NAME =
        Field.builder("name").stringField().mustNotBeEmpty().required();
    private static final Field.Optional<List<BsonDocument>> EFFECTIVE_PIPELINE =
        Field.builder("effectivePipeline")
            .listOf(Value.builder().documentValue().required())
            .optional()
            .noDefault();
    private static final Field.Required<Boolean> EXISTS =
        Field.builder("exists").booleanField().required();
  }

  public enum SupportedStage {
    ADD_FIELDS("$addFields"),
    SET("$set"),
    MATCH("$match");

    public final String stageName;

    SupportedStage(String stageName) {
      this.stageName = stageName;
    }

    public static Optional<SupportedStage> byStageName(String stageName) {

      for (var stage : values()) {
        if (stage.stageName.equals(stageName)) {
          return Optional.of(stage);
        }
      }

      return Optional.empty();
    }
  }

  private final String name;

  /**
   * The aggregation pipeline configured for the view, or a concatenation of several pipelines if
   * this view has parent views.
   */
  private final Optional<List<BsonDocument>> effectivePipeline;

  /** Set to false iff the view was deleted on mongod side */
  private final boolean exists;

  private ViewDefinition(
      String name, Optional<List<BsonDocument>> effectivePipeline, boolean exists) {
    checkArg(
        !exists || effectivePipeline.isPresent(),
        "effectivePipeline is required if the view exists");
    this.name = name;
    this.effectivePipeline = effectivePipeline;
    this.exists = exists;
  }

  public static ViewDefinition existing(String name, List<BsonDocument> pipeline) {
    return new ViewDefinition(name, Optional.of(pipeline), true);
  }

  public static ViewDefinition missing(String name) {
    return new ViewDefinition(name, Optional.empty(), false);
  }

  /**
   * Creates a view definition based on its namespace and provided collectionInfos. View can be
   * based on another view, so we iterate over the full hierarchy to concatenate the effective
   * pipeline. Note that mongodb does not allow cycles, so we are not checking it.
   *
   * @param viewNamespace db and view name
   * @param sourceCollectionUuid used to check if the view source still matches the expected one
   * @param collectionInfos collection infos that should contain the view and its parent hierarchy
   * @return ViewDefinition with effective pipeline or a ViewDefinition with empty pipeline and
   *     exists flag set to false, if it cannot be created from the supplied collection infos
   */
  public static ViewDefinition fromCollectionInfos(
      MongoNamespace viewNamespace,
      UUID sourceCollectionUuid,
      MongoDbCollectionInfos collectionInfos) {

    if (collectionInfos.getCollectionInfo(viewNamespace).isEmpty()
        || !(collectionInfos.getCollectionInfo(viewNamespace).get()
            instanceof MongoDbCollectionInfo.View viewInfo)) {
      return new ViewDefinition(viewNamespace.getCollectionName(), Optional.empty(), false);
    }

    @Var String viewOn = viewInfo.options().viewOn();
    @Var List<BsonDocument> pipeline = viewInfo.options().pipeline();

    while (true) {

      MongoNamespace parentNamespace = new MongoNamespace(viewNamespace.getDatabaseName(), viewOn);

      if (collectionInfos.getCollectionInfo(parentNamespace).isEmpty()) {
        return ViewDefinition.missing(viewNamespace.getCollectionName());
      }

      MongoDbCollectionInfo next = collectionInfos.getCollectionInfo(parentNamespace).get();

      switch (next) {
        case MongoDbCollectionInfo.Collection nextCollection:
          if (!Objects.equals(nextCollection.info().uuid(), sourceCollectionUuid)) {
            // the source collection is mismatched with expected one, which is equivalent to a
            // missing view. likely caused by changing viewOn after the index creation
            return ViewDefinition.missing(viewNamespace.getCollectionName());
          }
          return ViewDefinition.existing(viewNamespace.getCollectionName(), pipeline);
        case MongoDbCollectionInfo.View nextView:
          viewOn = nextView.options().viewOn();
          pipeline = ListUtils.union(nextView.options().pipeline(), pipeline);
          break;
      }
    }
  }

  public String getName() {
    return this.name;
  }

  public Optional<List<BsonDocument>> getEffectivePipeline() {
    return this.effectivePipeline;
  }

  public boolean exists() {
    return this.exists;
  }

  public static ViewDefinition fromBson(DocumentParser parser) throws BsonParseException {

    if (parser.getField(Fields.EXISTS).unwrap()
        && parser.getField(Fields.EFFECTIVE_PIPELINE).unwrap().isEmpty()) {
      parser
          .getContext()
          .handleSemanticError(
              String.format(
                  "View %s exists but the pipeline is not present",
                  parser.getField(Fields.NAME).unwrap()));
    }

    return new ViewDefinition(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.EFFECTIVE_PIPELINE).unwrap(),
        parser.getField(Fields.EXISTS).unwrap());
  }

  public static Optional<ViewDefinition> fromIndexDefinitionBson(BsonDocument indexDefinition)
      throws BsonParseException {

    if (!indexDefinition.containsKey(VIEW.getName())) {
      return Optional.empty();
    }

    var parser =
        BsonDocumentParser.fromRoot(indexDefinition.getDocument(VIEW.getName()))
            .allowUnknownFields(false)
            .build();

    return Optional.of(fromBson(parser));
  }

  @Override
  public BsonValue toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.EFFECTIVE_PIPELINE, this.effectivePipeline)
        .field(Fields.EXISTS, this.exists)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ViewDefinition)) {
      return false;
    }
    ViewDefinition that = (ViewDefinition) o;
    return this.exists == that.exists
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.effectivePipeline, that.effectivePipeline);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.effectivePipeline, this.exists);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("name", this.name)
        .append("effectivePipeline", this.effectivePipeline)
        .append("exists", this.exists)
        .toString();
  }
}
