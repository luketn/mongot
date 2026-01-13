package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record ExplainDefinition(Explain.Verbosity verbosity) implements DocumentEncodable {
  public static class Fields {
    public static final Field.Required<Explain.Verbosity> VERBOSITY =
        Field.builder("verbosity").enumField(Explain.Verbosity.class).asCamelCase().required();
  }

  public static ExplainDefinition fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  public static ExplainDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new ExplainDefinition(parser.getField(Fields.VERBOSITY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.VERBOSITY, this.verbosity).build();
  }
}
