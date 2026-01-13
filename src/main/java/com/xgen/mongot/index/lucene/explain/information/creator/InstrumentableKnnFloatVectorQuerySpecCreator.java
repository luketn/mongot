package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.InstrumentableKnnFloatVectorQuerySpec;
import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnFloatVectorQuery;

public class InstrumentableKnnFloatVectorQuerySpecCreator {
  static InstrumentableKnnFloatVectorQuerySpec fromQuery(InstrumentableKnnFloatVectorQuery query) {
    return new InstrumentableKnnFloatVectorQuerySpec(query.getField(), query.getK());
  }
}
