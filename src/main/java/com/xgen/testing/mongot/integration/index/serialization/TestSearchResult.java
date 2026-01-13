package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.ScoreDetails;
import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchResult;
import com.xgen.mongot.index.VectorSearchResult;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class TestSearchResult {

  private final Optional<BsonValue> id;
  private final float score;
  private final Optional<List<SearchHighlight>> searchHighlights;
  private final Optional<ScoreDetails> scoreDetails;
  private final Optional<BsonDocument> storedSource;
  private final Optional<BsonValue> searchRootDocumentId;

  public TestSearchResult(
      Optional<BsonValue> id,
      float score,
      Optional<List<SearchHighlight>> searchHighlights,
      Optional<ScoreDetails> scoreDetails,
      Optional<BsonDocument> storedSource,
      Optional<BsonValue> searchRootDocumentId) {
    this.id = id;
    this.score = score;
    this.searchHighlights = searchHighlights;
    this.scoreDetails = scoreDetails;
    this.storedSource = storedSource;
    this.searchRootDocumentId = searchRootDocumentId;
  }

  public TestSearchResult(SearchResult result) {
    this.id = result.id();
    this.score = result.score();
    this.searchHighlights = result.searchHighlights();
    this.scoreDetails = result.scoreDetails();
    this.storedSource = result.storedSource();
    this.searchRootDocumentId = result.searchRootDocumentId();
  }

  public TestSearchResult(VectorSearchResult result) {
    this.id = Optional.of(result.id());
    this.score = result.vectorSearchScore();
    this.searchHighlights = Optional.empty();
    this.scoreDetails = Optional.empty();
    this.storedSource = result.storedSource();
    this.searchRootDocumentId = Optional.empty();
  }

  public Optional<BsonValue> getId() {
    return this.id;
  }

  public float getScore() {
    return this.score;
  }

  public Optional<List<SearchHighlight>> getSearchHighlights() {
    return this.searchHighlights;
  }

  public Optional<ScoreDetails> getScoreDetails() {
    return this.scoreDetails;
  }

  public Optional<BsonDocument> getStoredSource() {
    return this.storedSource;
  }

  public Optional<BsonValue> getSearchRootDocumentId() {
    return this.searchRootDocumentId;
  }
}
