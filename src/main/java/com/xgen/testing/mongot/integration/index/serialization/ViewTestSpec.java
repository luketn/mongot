package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ViewTestSpec implements Encodable {

  private static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();

    static final Field.Optional<String> DESCRIPTION =
        Field.builder("description").stringField().optional().noDefault();

    static final Field.Required<IndexSpec> INDEX =
        Field.builder("index").classField(IndexSpec::fromBson).disallowUnknownFields().required();

    static final Field.WithDefault<List<BsonDocument>> INSERTS =
        Field.builder("inserts")
            .documentField()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.WithDefault<List<BsonDocument>> UPDATES =
        Field.builder("updates")
            .documentField()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.WithDefault<List<BsonDocument>> VIEW_PIPELINE =
        Field.builder("viewPipeline")
            .documentField()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.Required<BsonDocument> QUERY =
        Field.builder("query").documentField().required();

    static final Field.Optional<ValidResult> RESULT =
        Field.builder("result")
            .classField(ValidResult::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<List<BsonDocument>> RESULT_AFTER_ID_LOOKUP =
        Field.builder("resultAfterIdLookup").documentField().asList().optional().noDefault();
  }

  public final String name;

  @SuppressWarnings("unused")
  public final Optional<String> description;

  public final IndexSpec index;
  public final List<BsonDocument> inserts;
  public final List<BsonDocument> updates;
  public final List<BsonDocument> viewPipeline;
  public final BsonDocument query;
  public final Optional<ValidResult> result;
  public final Optional<List<BsonDocument>> resultAfterIdLookup;

  public ViewTestSpec(
      String name,
      Optional<String> description,
      IndexSpec index,
      List<BsonDocument> inserts,
      List<BsonDocument> updates,
      List<BsonDocument> viewPipeline,
      BsonDocument query,
      Optional<ValidResult> result,
      Optional<List<BsonDocument>> resultAfterIdLookup) {
    this.description = description;
    this.name = name;
    this.index = index;
    this.inserts = inserts;
    this.updates = updates;
    this.viewPipeline = viewPipeline;
    this.query = query;
    this.result = result;
    this.resultAfterIdLookup = resultAfterIdLookup;
  }

  public static ViewTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new ViewTestSpec(
        parser.getField(ViewTestSpec.Fields.NAME).unwrap(),
        parser.getField(ViewTestSpec.Fields.DESCRIPTION).unwrap(),
        parser.getField(ViewTestSpec.Fields.INDEX).unwrap(),
        parser.getField(ViewTestSpec.Fields.INSERTS).unwrap(),
        parser.getField(ViewTestSpec.Fields.UPDATES).unwrap(),
        parser.getField(ViewTestSpec.Fields.VIEW_PIPELINE).unwrap(),
        parser.getField(ViewTestSpec.Fields.QUERY).unwrap(),
        parser.getField(ViewTestSpec.Fields.RESULT).unwrap(),
        parser.getField(ViewTestSpec.Fields.RESULT_AFTER_ID_LOOKUP).unwrap());
  }

  @Override
  public BsonValue toBson() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return this.name;
  }
}
