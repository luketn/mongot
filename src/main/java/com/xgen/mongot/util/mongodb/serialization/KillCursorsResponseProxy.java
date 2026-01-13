package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class KillCursorsResponseProxy implements Bson {

  private static final String OK_FIELD = "ok";
  private static final String CURSORS_KILLED_FIELD = "cursorsKilled";
  private static final String CURSORS_NOT_FOUND_FIELD = "cursorsNotFound";
  private static final String CURSORS_ALIVE_FIELD = "cursorsAlive";
  private static final String CURSORS_UNKNOWN_FIELD = "cursorsUnknown";

  private final double ok;
  private final List<Long> cursorsKilled;
  private final List<Long> cursorsNotFound;
  private final List<Long> cursorsAlive;
  private final List<Long> cursorsUnknown;

  public KillCursorsResponseProxy(
      double ok,
      List<Long> cursorsKilled,
      List<Long> cursorsNotFound,
      List<Long> cursorsAlive,
      List<Long> cursorsUnknown) {
    Check.argNotNull(cursorsKilled, "cursorsKilled");
    Check.argNotNull(cursorsNotFound, "cursorsNotFound");
    Check.argNotNull(cursorsAlive, "cursorsAlive");
    Check.argNotNull(cursorsUnknown, "cursorsUnknown");

    this.ok = ok;
    this.cursorsKilled = cursorsKilled;
    this.cursorsNotFound = cursorsNotFound;
    this.cursorsAlive = cursorsAlive;
    this.cursorsUnknown = cursorsUnknown;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    return new BsonDocument()
        .append(OK_FIELD, new BsonDouble(this.ok))
        .append(CURSORS_KILLED_FIELD, cursorsListToBsonArray(this.cursorsKilled))
        .append(CURSORS_NOT_FOUND_FIELD, cursorsListToBsonArray(this.cursorsNotFound))
        .append(CURSORS_ALIVE_FIELD, cursorsListToBsonArray(this.cursorsAlive))
        .append(CURSORS_UNKNOWN_FIELD, cursorsListToBsonArray(this.cursorsUnknown));
  }

  private static BsonArray cursorsListToBsonArray(List<Long> cursors) {
    return cursors.stream().map(BsonInt64::new).collect(Collectors.toCollection(BsonArray::new));
  }
}
