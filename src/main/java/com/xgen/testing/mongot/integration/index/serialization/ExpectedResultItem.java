package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.ScoreDetails;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ExpectedResultItem implements ExpectedResult {

  private static class Fields {
    private static final Field.Optional<Integer> ID =
        Field.builder("_id").intField().optional().noDefault();

    private static final Field.Optional<Float> SCORE =
        Field.builder("score").floatField().optional().noDefault();

    private static final Field.Optional<ScoreDetails> SCORE_DETAILS =
        Field.builder("scoreDetails")
            .classField(ScoreDetails::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<List<ExpectedSearchHighlight>> SEARCH_HIGHLIGHTS =
        Field.builder("searchHighlights")
            .classField(ExpectedSearchHighlight::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    private static final Field.Optional<BsonValue> STORED_SOURCE =
        Field.builder("storedSource").unparsedValueField().optional().noDefault();

    /** Generated for search queries using returnScope */
    private static final Field.Optional<BsonValue> SEARCH_ROOT_DOCUMENT_ID =
        Field.builder("$searchRootDocumentId").unparsedValueField().optional().noDefault();
  }

  private final Optional<Integer> id;
  private final Optional<Float> score;
  private final Optional<ScoreDetails> scoreDetails;
  private final Optional<List<ExpectedSearchHighlight>> searchHighlights;
  private final Optional<BsonValue> storedSource;
  private final Optional<BsonValue> searchRootDocumentId;

  private ExpectedResultItem(
      Optional<Integer> id,
      Optional<Float> score,
      Optional<ScoreDetails> scoreDetails,
      Optional<List<ExpectedSearchHighlight>> searchHighlights,
      Optional<BsonValue> storedSource,
      Optional<BsonValue> searchRootDocumentId) {
    this.id = id;
    this.score = score;
    this.searchHighlights = searchHighlights;
    this.storedSource = storedSource;
    this.scoreDetails = scoreDetails;
    this.searchRootDocumentId = searchRootDocumentId;
  }

  static ExpectedResultItem fromBson(DocumentParser parser) throws BsonParseException {
    return new ExpectedResultItem(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.SCORE).unwrap(),
        parser.getField(Fields.SCORE_DETAILS).unwrap(),
        parser.getField(Fields.SEARCH_HIGHLIGHTS).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.SEARCH_ROOT_DOCUMENT_ID).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.SCORE, this.score)
        .field(Fields.SCORE_DETAILS, this.scoreDetails)
        .field(Fields.SEARCH_HIGHLIGHTS, this.searchHighlights)
        .field(Fields.STORED_SOURCE, this.storedSource)
        .field(Fields.SEARCH_ROOT_DOCUMENT_ID, this.searchRootDocumentId)
        .build();
  }

  @Override
  public Type getType() {
    return Type.SINGLE_ITEM;
  }

  @Override
  public List<ExpectedResultItem> getResults() {
    return List.of(this);
  }

  public Optional<Integer> getId() {
    return this.id;
  }

  public Optional<Float> getScore() {
    return this.score;
  }

  public Optional<ScoreDetails> getScoreDetails() {
    return this.scoreDetails;
  }

  public Optional<List<ExpectedSearchHighlight>> getSearchHighlights() {
    return this.searchHighlights;
  }

  public Optional<BsonValue> getStoredSource() {
    return this.storedSource;
  }

  public Optional<BsonValue> getSearchRootDocumentId() {
    return this.searchRootDocumentId;
  }
}
