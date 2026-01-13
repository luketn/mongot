package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * The NorClause applies for the logical NOR operator. Given a list of clauses [a,b,c], performing
 * {$nor: [a,b,c]} will return all the documents such that neither a, b nor c evaluate to true.
 * Passing in an empty array of clauses is disallowed.
 */
public final class NorClause extends CompoundClause {
  public NorClause(List<Clause> clauses) {
    super(clauses);
    // The parser protects us from having empty clause.
    Check.checkState(!this.clauses.isEmpty(), "NorClause cannot have empty clause.");
  }

  public static NorClause bsonToNorClause(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    List<Clause> clauses = CompoundClause.Values.CLAUSES.getParser().parse(context, value);
    // The clauses won't be empty because CLAUSES has mustNotBeEmpty().
    return new NorClause(clauses);
  }

  @Override
  public Operator getOperator() {
    return Operator.NOR;
  }

  @Override
  public List<Clause> getClauses() {
    return this.clauses;
  }

  @Override
  public BsonDocument toBson() {
    return new BsonDocument(
        NOR_FIELD_KEY, CompoundClause.Values.CLAUSES.getEncoder().encode(this.clauses));
  }
}
