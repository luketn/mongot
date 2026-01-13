package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.ScoreDetails;
import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchResult;
import com.xgen.mongot.index.SearchSortValues;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class SearchResultBuilder {
  private Optional<BsonValue> id = Optional.empty();
  private Optional<Float> score = Optional.empty();
  private Optional<List<SearchHighlight>> highlights = Optional.empty();
  private Optional<BsonDocument> storedSource = Optional.empty();
  private Optional<ScoreDetails> scoreDetails = Optional.empty();

  private Optional<SearchSortValues> searchSortValues = Optional.empty();

  private Optional<SequenceToken> sequenceToken = Optional.empty();
  private Optional<BsonValue> searchRootDocumentId = Optional.empty();

  public static SearchResultBuilder builder() {
    return new SearchResultBuilder();
  }

  public SearchResultBuilder id(BsonValue id) {
    this.id = Optional.of(id);
    return this;
  }

  public SearchResultBuilder score(Float score) {
    this.score = Optional.of(score);
    return this;
  }

  public SearchResultBuilder sequenceToken(SequenceToken token) {
    this.sequenceToken = Optional.of(token);
    return this;
  }

  public SearchResultBuilder highlights(List<SearchHighlight> highlights) {
    this.highlights = Optional.of(highlights);
    return this;
  }

  public SearchResultBuilder storedSource(BsonDocument storedSource) {
    this.storedSource = Optional.of(storedSource);
    return this;
  }

  public SearchResultBuilder scoreDetails(ScoreDetails scoreDetails) {
    this.scoreDetails = Optional.of(scoreDetails);
    return this;
  }

  public SearchResultBuilder searchSortValues(SearchSortValues searchSortValues) {
    this.searchSortValues = Optional.of(searchSortValues);
    return this;
  }

  public SearchResultBuilder searchRootDocumentId(BsonValue searchRootDocumentId) {
    this.searchRootDocumentId = Optional.of(searchRootDocumentId);
    return this;
  }

  public SearchResult build() {
    Check.isPresent(this.score, "score");
    return new SearchResult(
        this.id,
        this.score.get(),
        this.highlights,
        this.scoreDetails,
        this.storedSource,
        this.searchSortValues,
        this.sequenceToken,
        this.searchRootDocumentId);
  }
}
