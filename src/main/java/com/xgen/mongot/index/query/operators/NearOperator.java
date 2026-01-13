package com.xgen.mongot.index.query.operators;

import static com.xgen.mongot.index.query.points.Point.Type.DATE;
import static com.xgen.mongot.index.query.points.Point.Type.GEO;
import static com.xgen.mongot.index.query.points.Point.Type.NUMBER;

import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonValue;

/**
 * A predicate that scores based on proximity to an origin. Score is equal to (pivot)/(pivot +
 * abs(origin - x)) where "x" is a document value at path.
 *
 * @param score score specification.
 * @param paths path(s) to consider values at.
 * @param origin highest-scoring value. documents are scored based on their distance from origin.
 * @param pivot distance from origin where scoring is halved
 */
public record NearOperator(Score score, List<FieldPath> paths, Point origin, double pivot)
    implements Operator {

  private static final Set<Point.Type> SUPPORTED_POINT_TYPES = EnumSet.of(DATE, GEO, NUMBER);

  public static class Fields {
    public static final Field.Required<Point> ORIGIN =
        Field.builder("origin")
            .classField(Point::fromBson)
            .validate(
                point ->
                    SUPPORTED_POINT_TYPES.contains(point.getType())
                        ? Optional.empty()
                        : Optional.of("Unsupported type: " + point.getType()))
            .required();

    public static final Field.Required<Double> PIVOT =
        Field.builder("pivot").doubleField().mustBeFinite().mustBePositive().required();
  }

  @Override
  public Type getType() {
    return Type.NEAR;
  }

  public static NearOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new NearOperator(
        Operators.parseScore(parser),
        Operators.parseFieldPath(parser),
        parser.getField(Fields.ORIGIN).unwrap(),
        parser.getField(Fields.PIVOT).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score(), this.paths)
        .field(Fields.ORIGIN, this.origin)
        .field(Fields.PIVOT, this.pivot)
        .build();
  }
}
