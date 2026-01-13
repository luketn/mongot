package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ZoneRange implements DocumentEncodable {

  static class Fields {
    static final Field.Required<BsonValue> START =
        Field.builder("start").unparsedValueField().required();

    static final Field.Required<BsonValue> END =
        Field.builder("end").unparsedValueField().required();
  }

  private final BsonValue start;
  private final BsonValue end;

  private ZoneRange(BsonValue start, BsonValue end) {
    this.start = start;
    this.end = end;
  }

  static ZoneRange fromBson(DocumentParser parser) throws BsonParseException {
    return new ZoneRange(
        parser.getField(ZoneRange.Fields.START).unwrap(),
        parser.getField(ZoneRange.Fields.END).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.START, this.start)
        .field(Fields.END, this.end)
        .build();
  }

  public BsonValue getStart() {
    return this.start;
  }

  public BsonValue getEnd() {
    return this.end;
  }
}
