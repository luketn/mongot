package com.xgen.mongot.index.query.operators.mql;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Class that deserializes mql clauses. */
public sealed interface Clause extends Encodable permits SimpleClause, CompoundClause {
  // This method could have been a FromDocumentParser but the signature only allows a
  // DocumentParser, and currently we do not have a way to access the document being parsed from the
  // BsonDocumentParser. That's why this method is a FromValueParser
  public static Clause fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    if (!value.isDocument()) {
      context.handleSemanticError("filter must be a document");
    }
    BsonDocument topDocument = value.asDocument();
    ImmutableList.Builder<Clause> clausesBuilder =
        ImmutableList.builderWithExpectedSize(topDocument.size());
    // Set allowUnknownFields to true, because the `parser` is only used to parse the SimpleClause.
    // The And/OrClause is directly parsed by using the `BsonParseContext`, so the `parser`
    // internally watcher is unaware of the And/OrClause fields. Ideally we can unify these two
    // levels of API usage.
    try (var parser =
        BsonDocumentParser.withContext(context, topDocument).allowUnknownFields(true).build()) {
      for (String key : topDocument.keySet()) {
        // This is still not the simplest design. If we can represent AND/OR state
        // inside the CompoundClause, the first two branches can be saved.
        switch (key) {
          case CompoundClause.AND_FIELD_KEY ->
              // This function only parses the value, but not the key.
              clausesBuilder.add(AndClause.bsonToExplicitAndClause(context, topDocument.get(key)));
          case CompoundClause.OR_FIELD_KEY ->
              // This function only parses the value, but not the key.
              clausesBuilder.add(OrClause.bsonToOrClause(context, topDocument.get(key)));
          case CompoundClause.NOR_FIELD_KEY ->
              // This function only parses the value, but not the key.
              clausesBuilder.add(NorClause.bsonToNorClause(context, topDocument.get(key)));
          default ->
              clausesBuilder.add(
                  SimpleClause.bsonToSimpleClause(context, parser, key, topDocument.get(key)));
        }
      }
    }
    return AndClause.createImplicitAndClause(clausesBuilder.build());
  }
}
