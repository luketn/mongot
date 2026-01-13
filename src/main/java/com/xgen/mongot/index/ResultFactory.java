package com.xgen.mongot.index;

import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

@FunctionalInterface
public interface ResultFactory {
  DocumentEncodable create(
      Optional<BsonValue> id,
      float score,
      Optional<List<SearchHighlight>> searchHighlights,
      Optional<ScoreDetails> scoreDetails,
      Optional<BsonDocument> storedSource,
      Optional<SearchSortValues> searchSortValues,
      Optional<SequenceToken> sequenceToken,
      Optional<BsonValue> searchRootDocumentId);
}
