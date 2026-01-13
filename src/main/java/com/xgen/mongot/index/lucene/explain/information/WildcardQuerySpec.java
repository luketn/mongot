package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class WildcardQuerySpec extends AbstractFieldValueQuerySpec {

  public WildcardQuerySpec(FieldPath fieldPath, String term) {
    super(Type.WILDCARD_QUERY, fieldPath, term);
  }

  public static WildcardQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new WildcardQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }
}
