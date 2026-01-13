package com.xgen.mongot.index;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import java.io.IOException;
import org.bson.BsonArray;

public interface VectorIndexReader extends IndexReader {

  /** Returns a bson array of {@link VectorSearchResult}. */
  BsonArray query(MaterializedVectorSearchQuery materializedQuery)
      throws ReaderClosedException, IOException, InvalidQueryException;
}
