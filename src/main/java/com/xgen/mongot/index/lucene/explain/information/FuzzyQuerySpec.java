package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class FuzzyQuerySpec extends AbstractFieldValueQuerySpec {

  public FuzzyQuerySpec(FieldPath fieldPath, String term) {
    super(Type.FUZZY_QUERY, fieldPath, term);
  }

  public static FuzzyQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new FuzzyQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }
}
