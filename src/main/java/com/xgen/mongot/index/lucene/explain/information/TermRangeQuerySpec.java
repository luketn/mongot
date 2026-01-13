package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class TermRangeQuerySpec extends AbstractFieldValueQuerySpec {

  public TermRangeQuerySpec(FieldPath fieldPath, String interval) {
    super(Type.TERM_RANGE_QUERY, fieldPath, interval);
  }

  static TermRangeQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new TermRangeQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }
}
