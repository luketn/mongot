package com.xgen.mongot.index.ingestion.parsers;

import com.xgen.mongot.util.FloatCollector;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.bson.BsonReader;
import org.bson.BsonType;

public class KnnVectorParser {

  /**
   * Iterates through the array and performs narrowing primitive conversion to each element. Note
   * that according to the Java Language Specification, this conversion can lose precision and
   * range, result in a float zero from a nonzero double and a float infinity from a finite double:
   * https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.1.3.
   */
  public static Optional<Vector> parse(BsonReader reader) {

    var vector = new FloatCollector();
    reader.readStartArray();

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.getCurrentBsonType()) {
        case INT32:
          vector.add((float) reader.readInt32());
          break;
        case INT64:
          vector.add((float) reader.readInt64());
          break;
        case DOUBLE:
          vector.add((float) reader.readDouble());
          break;
        default:
          return Optional.empty();
      }
    }
    reader.readEndArray();

    return Optional.of(Vector.fromFloats(vector.toArray(), FloatVector.OriginalType.NATIVE));
  }
}
