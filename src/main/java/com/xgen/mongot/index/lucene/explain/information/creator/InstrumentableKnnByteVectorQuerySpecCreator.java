package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.InstrumentableKnnByteVectorQuerySpec;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnByteVectorQuery;

public class InstrumentableKnnByteVectorQuerySpecCreator {
  static InstrumentableKnnByteVectorQuerySpec fromQuery(InstrumentableKnnByteVectorQuery query) {
    return new InstrumentableKnnByteVectorQuerySpec(query.getField(), query.getK());
  }
}
