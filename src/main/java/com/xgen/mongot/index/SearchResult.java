package com.xgen.mongot.index;

import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * SearchResult is a successful result from a Query. Either _id or storedSource is guaranteed to be
 * present.
 */
public record SearchResult(
    Optional<BsonValue> id,
    float score,
    Optional<List<SearchHighlight>> searchHighlights,
    Optional<ScoreDetails> scoreDetails,
    Optional<BsonDocument> storedSource,
    Optional<SearchSortValues> searchSortValues,
    Optional<SequenceToken> sequenceToken,
    Optional<BsonValue> searchRootDocumentId)
    implements DocumentEncodable {

  private static class Fields {
    private static final Field.Optional<BsonValue> ID =
        Field.builder("_id").unparsedValueField().optional().noDefault();

    private static final Field.Required<Float> SCORE =
        Field.builder("$searchScore").floatField().required();

    private static final Field.Optional<List<SearchHighlight>> HIGHLIGHTS =
        Field.builder("$searchHighlights")
            .classField(SearchHighlight::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    private static final Field.Optional<ScoreDetails> SCORE_DETAILS =
        Field.builder("$searchScoreDetails")
            .classField(ScoreDetails::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<BsonDocument> STORED_SOURCE =
        Field.builder("storedSource").documentField().optional().noDefault();

    private static final Field.Optional<SearchSortValues> SEARCH_SORT_VALUES =
        Field.builder("$searchSortValues")
            .classField(SearchSortValues::fromBson)
            .optional()
            .noDefault();

    /** Will only be generated for paginated queries, indicated by requireSequenceTokens=true. */
    private static final Field.Optional<SequenceToken> SEQUENCE_TOKEN =
        Field.builder("$searchSequenceToken")
            .classField(SequenceToken::fromBson)
            .optional()
            .noDefault();

    /** Generated for search queries using returnScope */
    private static final Field.Optional<BsonValue> SEARCH_ROOT_DOCUMENT_ID =
        Field.builder("$searchRootDocumentId").unparsedValueField().optional().noDefault();
  }

  public SearchResult(
      Optional<BsonValue> id,
      float score,
      Optional<List<SearchHighlight>> searchHighlights,
      Optional<ScoreDetails> scoreDetails,
      Optional<BsonDocument> storedSource,
      Optional<SearchSortValues> searchSortValues,
      Optional<SequenceToken> sequenceToken,
      Optional<BsonValue> searchRootDocumentId) {

    this.id = id;
    this.score = score;
    this.searchHighlights = searchHighlights;
    this.scoreDetails = scoreDetails;
    this.storedSource = storedSource;
    this.searchSortValues = searchSortValues;
    this.sequenceToken = sequenceToken;
    this.searchRootDocumentId = searchRootDocumentId;

    if (Float.isNaN(score)) {
      throw new IllegalArgumentException("score is NaN");
    }

    if (id.isEmpty() && storedSource.isEmpty()) {
      throw new IllegalArgumentException("Either id or storedSource should be present");
    }
  }

  public static SearchResult fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  public static SearchResult fromBson(DocumentParser parser) throws BsonParseException {
    parser
        .getGroup()
        .atLeastOneOf(parser.getField(Fields.ID), parser.getField(Fields.STORED_SOURCE));
    return new SearchResult(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.SCORE).unwrap(),
        parser.getField(Fields.HIGHLIGHTS).unwrap(),
        parser.getField(Fields.SCORE_DETAILS).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.SEARCH_SORT_VALUES).unwrap(),
        parser.getField(Fields.SEQUENCE_TOKEN).unwrap(),
        parser.getField(Fields.SEARCH_ROOT_DOCUMENT_ID).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.SCORE, this.score)
        .field(Fields.HIGHLIGHTS, this.searchHighlights)
        .field(Fields.SCORE_DETAILS, this.scoreDetails)
        .field(Fields.STORED_SOURCE, this.storedSource)
        .field(Fields.SEARCH_SORT_VALUES, this.searchSortValues)
        .field(Fields.SEQUENCE_TOKEN, this.sequenceToken)
        .field(Fields.SEARCH_ROOT_DOCUMENT_ID, this.searchRootDocumentId)
        .build();
  }

  @Override
  public String toString() {
    return String.format("SearchResult(id=%s, score=%.2f)", this.id.orElse(null), this.score);
  }
}
