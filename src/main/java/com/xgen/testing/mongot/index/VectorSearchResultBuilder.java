package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class VectorSearchResultBuilder {
  private Optional<BsonValue> id = Optional.empty();
  private Optional<Float> vectorSearchScore = Optional.empty();
  private Optional<BsonDocument> storedSource = Optional.empty();

  public static VectorSearchResultBuilder builder() {
    return new VectorSearchResultBuilder();
  }

  public VectorSearchResultBuilder id(BsonValue id) {
    this.id = Optional.of(id);
    return this;
  }

  public VectorSearchResultBuilder score(Float vectorSearchScore) {
    this.vectorSearchScore = Optional.of(vectorSearchScore);
    return this;
  }

  public VectorSearchResultBuilder storedSource(BsonDocument storedSource) {
    this.storedSource = Optional.of(storedSource);
    return this;
  }

  public VectorSearchResult build() {
    Check.isPresent(this.id, "id");
    Check.isPresent(this.vectorSearchScore, "vectorSearchScore");
    return new VectorSearchResult(this.id.get(), this.vectorSearchScore.get(), this.storedSource);
  }
}
