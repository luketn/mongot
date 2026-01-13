package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.bson.BsonSerialization;
import java.util.Optional;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class CollectionStatsResponseProxy {

  private static final String OK_FIELD = "ok";
  private static final String COUNT_FIELD = "count";
  private static final String AVG_OBJ_SIZE_FIELD = "avgObjSize";

  private final double ok;
  private final long count;
  private final Optional<Integer> avgObjSize;

  /** Constructs a new CollectionStatsReponseProxy. */
  @BsonCreator
  public CollectionStatsResponseProxy(
      @BsonProperty(OK_FIELD) Double ok,
      @BsonProperty(COUNT_FIELD) Long count,
      @BsonProperty(AVG_OBJ_SIZE_FIELD) Integer avgObjSize) {
    BsonSerialization.throwIfMissingProperty(ok, "ok");
    BsonSerialization.throwIfMissingProperty(count, "count");
    this.ok = ok;
    this.count = count;
    this.avgObjSize = Optional.ofNullable(avgObjSize);
  }

  public double getOk() {
    return this.ok;
  }

  public long getCount() {
    return this.count;
  }

  public int getAvgObjSize() {
    return this.avgObjSize.orElse(0);
  }
}
