package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

public class FtdcMetadata {

  public static final String START_KEY = "start";
  public static final String TYPE_KEY = "type";

  private final ImmutableMap<String, BsonValue> staticMetadata;

  private FtdcMetadata(Map<String, BsonValue> staticMetadata) {
    this.staticMetadata = ImmutableMap.copyOf(staticMetadata);
  }

  /** Serializes the FtdcMetadata and includes the type and start keys. */
  public BsonDocument serialize() {
    BsonDocument metadataBson =
        new BsonDocument()
            .append(START_KEY, new BsonDateTime(new Date().getTime()))
            .append(TYPE_KEY, new BsonString("mongot"));

    metadataBson.putAll(this.staticMetadata);
    return metadataBson;
  }

  public static class Builder {
    private static final ImmutableSet<String> RESERVED_KEYS = ImmutableSet.of(START_KEY, TYPE_KEY);

    private final Map<String, BsonValue> staticMetadata;

    public Builder() {
      this.staticMetadata = new HashMap<>();
    }

    /** Adds static metadata information to FtdcMetadataBuilder. */
    public Builder addStaticInfo(String key, BsonValue value) {
      checkArg(!RESERVED_KEYS.contains(key), "key cannot be '%s'", key);
      checkArg(!this.staticMetadata.containsKey(key), "can only add metadata information once");

      this.staticMetadata.put(key, value);
      return this;
    }

    /** Builds an FtdcMetadata from an FtdcMetadataBuilder. */
    public FtdcMetadata build() {
      return new FtdcMetadata(this.staticMetadata);
    }
  }
}
