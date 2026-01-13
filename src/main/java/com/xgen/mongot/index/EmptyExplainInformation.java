package com.xgen.mongot.index;

import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.Objects;
import org.bson.BsonDocument;

public class EmptyExplainInformation implements DocumentEncodable {

  @Override
  public BsonDocument toBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EmptyExplainInformation;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
