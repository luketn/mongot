package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class InstrumentableKnnFloatVectorQuerySpec extends KnnVectorQuerySpec {

  public InstrumentableKnnFloatVectorQuerySpec(String field, int k) {
    super(field, k);
  }

  public static InstrumentableKnnFloatVectorQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new InstrumentableKnnFloatVectorQuerySpec(
        parser.getField(Fields.FIELD).unwrap(), parser.getField(Fields.K).unwrap());
  }

  @Override
  public LuceneQuerySpecification.Type getType() {
    return Type.INSTRUMENTABLE_KNN_FLOAT_VECTOR_QUERY;
  }
}
