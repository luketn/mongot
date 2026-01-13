package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public record Coordinate(List<Double> coord) implements Encodable {
  static class Values {
    static final Value.Required<List<Double>> COORD =
        Value.builder()
            .doubleValue()
            .asList()
            .validate(
                coordinates ->
                    coordinates.size() == 2
                        ? Optional.empty()
                        : Optional.of("coordinates must have exactly 2 values"))
            .required();
  }

  static Coordinate fromBson(BsonParseContext context, BsonValue value) throws BsonParseException {
    List<Double> coord = Values.COORD.getParser().parse(context, value);

    return new Coordinate(coord);
  }

  @Override
  public BsonValue toBson() {
    List<BsonDouble> bsonCoord =
        this.coord.stream().map(BsonDouble::new).collect(Collectors.toList());
    return new BsonArray(bsonCoord);
  }
}
