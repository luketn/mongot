package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonValue;

/** Intermediary class for serializing/deserializing a list of MqlFilterOperators. */
public record MqlFilterOperatorList(List<MqlFilterOperator> mqlFilterOperators)
    implements Encodable {

  public static MqlFilterOperatorList fromBson(DocumentParser parser) throws BsonParseException {
    return new MqlFilterOperatorList(MqlFilterOperator.atLeastOneFromBson(parser));
  }

  @Override
  public BsonValue toBson() {
    // merge operators list back into mapping.
    List<BsonElement> elements = new ArrayList<>();
    for (var operator : this.mqlFilterOperators) {
      for (var entry : operator.toBson().entrySet()) {
        elements.add(new BsonElement(entry.getKey(), entry.getValue()));
      }
    }
    return new BsonDocument(elements);
  }
}
