package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.scores.Score;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;

public class MoreLikeThisOperatorBuilder {
  List<BsonDocument> docs = new ArrayList<>();

  public MoreLikeThisOperatorBuilder like(BsonDocument likeDoc) {
    this.docs.add(likeDoc);
    return this;
  }

  public MoreLikeThisOperatorBuilder() {}

  public MoreLikeThisOperator build() {
    return new MoreLikeThisOperator(this.docs, Score.defaultScore());
  }
}
