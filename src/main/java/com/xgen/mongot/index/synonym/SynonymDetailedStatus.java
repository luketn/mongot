package com.xgen.mongot.index.synonym;

import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

public record SynonymDetailedStatus(SynonymStatus statusCode, Optional<String> message)
    implements DocumentEncodable {

  public static class Fields {
    static final Field.Required<SynonymStatus> SYNONYM_STATUS =
        Field.builder("statusCode").enumField(SynonymStatus.class).asUpperUnderscore().required();

    static final Field.Optional<String> MESSAGE =
        Field.builder("message").stringField().optional().noDefault();
  }

  public static SynonymDetailedStatus fromBson(DocumentParser parser) throws BsonParseException {
    return new SynonymDetailedStatus(
        parser.getField(Fields.SYNONYM_STATUS).unwrap(), parser.getField(Fields.MESSAGE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SYNONYM_STATUS, this.statusCode)
        .field(Fields.MESSAGE, this.message)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SynonymDetailedStatus that = (SynonymDetailedStatus) o;
    return this.statusCode == that.statusCode && Objects.equals(this.message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.statusCode, this.message);
  }
}
