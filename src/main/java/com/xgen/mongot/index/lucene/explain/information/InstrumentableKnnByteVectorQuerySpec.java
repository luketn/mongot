package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class InstrumentableKnnByteVectorQuerySpec extends KnnVectorQuerySpec {

  public InstrumentableKnnByteVectorQuerySpec(String field, int k) {
    super(field, k);
  }

  public static InstrumentableKnnByteVectorQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new InstrumentableKnnByteVectorQuerySpec(
        parser.getField(Fields.FIELD).unwrap(), parser.getField(Fields.K).unwrap());
  }

  @Override
  public Type getType() {
    return Type.INSTRUMENTABLE_KNN_BYTE_VECTOR_QUERY;
  }
}
