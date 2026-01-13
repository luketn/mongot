package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class OrClause extends CompoundClause {
  public OrClause(List<Clause> clauses) {
    super(clauses);
    // The parser protects us from having empty clause.
    Check.checkState(!this.clauses.isEmpty(), "OrClause cannot have empty clause.");
  }

  public static OrClause bsonToOrClause(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    List<Clause> clauses = CompoundClause.Values.CLAUSES.getParser().parse(context, value);
    // The clauses won't be empty because CLAUSES has mustNotBeEmpty().
    return new OrClause(clauses);
  }

  @Override
  public Operator getOperator() {
    return Operator.OR;
  }

  @Override
  public List<Clause> getClauses() {
    return this.clauses;
  }

  @Override
  public BsonDocument toBson() {
    return new BsonDocument(
        OR_FIELD_KEY, CompoundClause.Values.CLAUSES.getEncoder().encode(this.clauses));
  }
}
