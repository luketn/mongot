package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;

public class PrefixQuerySpec extends AbstractFieldValueQuerySpec {

  public PrefixQuerySpec(FieldPath fieldPath, String term) {
    super(Type.PREFIX_QUERY, fieldPath, term);
  }

  public static PrefixQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new PrefixQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }
}
