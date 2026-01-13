package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.util.bson.parser.BsonParseException;

public interface MqlFilterOperatorBuilder<T extends MqlFilterOperator> {
  static GtOperatorBuilder gt() {
    return new GtOperatorBuilder();
  }

  static GteOperatorBuilder gte() {
    return new GteOperatorBuilder();
  }

  static LtOperatorBuilder lt() {
    return new LtOperatorBuilder();
  }

  static LteOperatorBuilder lte() {
    return new LteOperatorBuilder();
  }

  static EqOperatorBuilder eq() {
    return new EqOperatorBuilder();
  }

  static NeOperatorBuilder ne() {
    return new NeOperatorBuilder();
  }

  static InOperatorBuilder in() {
    return new InOperatorBuilder();
  }

  static NinOperatorBuilder nin() {
    return new NinOperatorBuilder();
  }

  static NotOperatorBuilder not() {
    return new NotOperatorBuilder();
  }

  static ExistsOperatorBuilder exists() {
    return new ExistsOperatorBuilder();
  }

  T build() throws BsonParseException;
}
