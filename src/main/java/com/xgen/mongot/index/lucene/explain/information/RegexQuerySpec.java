package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class RegexQuerySpec extends AbstractFieldValueQuerySpec {

  public RegexQuerySpec(FieldPath fieldPath, String term) {
    super(Type.REGEX_QUERY, fieldPath, term);
  }

  public static RegexQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new RegexQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }
}
