package com.xgen.testing.mongot.index.ingestion.serialization;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.testing.mongot.integration.index.serialization.IndexSpec;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

public class BsonProcessorTestSpec implements DocumentEncodable {
  static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();

    static final Field.Optional<List<String>> DESCRIPTION =
        Field.builder("description").stringField().asSingleValueOrList().optional().noDefault();

    static final Field.Required<IndexSpec> INDEX =
        Field.builder("index").classField(IndexSpec::fromBson).disallowUnknownFields().required();

    static final Field.Required<BsonDocument> SOURCE_DOCUMENT =
        Field.builder("sourceDocument").documentField().required();

    static final Field.WithDefault<Map<String, Map<String, Vector>>> VECTOR_SIDE_INPUT =
        Field.builder("vector_side_input")
            .classField(Vector::fromBson)
            .asMap()
            .asMap()
            .optional()
            .withDefault(ImmutableMap.of());

    /**
     * Note this field is read by src/tools/scripts/testing/update_ingestion_test_json.py. If you
     * change the name here, please update the script as well.
     */
    static final Field.Required<List<Map<String, List<LuceneIndexedFieldSpec>>>> LUCENE_DOCUMENTS =
        Field.builder("luceneDocuments")
            .classField(LuceneIndexedFieldSpec::fromBson)
            .disallowUnknownFields()
            .asSingleValueOrList()
            .asMap()
            .asSingleValueOrList()
            .required();
  }

  private final String name;
  private final Optional<List<String>> description;
  private final IndexSpec index;
  private final BsonDocument sourceDocument;
  private final Map<String, Map<String, Vector>> vectorSideInput;
  private final List<Map<String, List<LuceneIndexedFieldSpec>>> luceneDocuments;

  public BsonProcessorTestSpec(
      String name,
      Optional<List<String>> description,
      IndexSpec index,
      BsonDocument sourceDocument,
      Map<String, Map<String, Vector>> vectorSideInput,
      List<Map<String, List<LuceneIndexedFieldSpec>>> luceneDocuments) {
    this.name = name;
    this.description = description;
    this.index = index;
    this.sourceDocument = sourceDocument;
    this.vectorSideInput = vectorSideInput;
    this.luceneDocuments = luceneDocuments;
  }

  static BsonProcessorTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new BsonProcessorTestSpec(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.DESCRIPTION).unwrap(),
        parser.getField(Fields.INDEX).unwrap(),
        parser.getField(Fields.SOURCE_DOCUMENT).unwrap(),
        parser.getField(Fields.VECTOR_SIDE_INPUT).unwrap(),
        parser.getField(Fields.LUCENE_DOCUMENTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.DESCRIPTION, this.description)
        .field(Fields.INDEX, this.index)
        .field(Fields.SOURCE_DOCUMENT, this.sourceDocument)
        .field(Fields.VECTOR_SIDE_INPUT, this.vectorSideInput)
        .field(Fields.LUCENE_DOCUMENTS, this.luceneDocuments)
        .build();
  }

  public String getName() {
    return this.name;
  }

  public Optional<List<String>> getDescription() {
    return this.description;
  }

  public IndexSpec getIndex() {
    return this.index;
  }

  public BsonDocument getSourceDocument() {
    return this.sourceDocument;
  }

  public Map<String, Map<String, Vector>> getVectorSideInput() {
    return this.vectorSideInput;
  }

  public List<Map<String, List<LuceneIndexedFieldSpec>>> getLuceneDocuments() {
    return this.luceneDocuments;
  }
}
