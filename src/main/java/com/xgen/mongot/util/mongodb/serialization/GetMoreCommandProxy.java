package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * GetMoreCommandProxy is a proxy for a <em>getMore</em> command.
 *
 * <p>See https://docs.mongodb.com/manual/reference/command/getMore
 */
public class GetMoreCommandProxy implements Bson {

  private static final String GET_MORE_FIELD = "getMore";
  private static final String COLLECTION_FIELD = "collection";
  private static final String BATCH_SIZE_FIELD = "batchSize";
  private static final String MAX_TIME_MS_FIELD = "maxTimeMS";

  private final long cursorId;
  private final String collection;
  private final Optional<Integer> batchSize;
  private final Optional<Integer> maxTimeMs;

  public GetMoreCommandProxy(
      long cursorId, String collection, Optional<Integer> batchSize, Optional<Integer> maxTimeMs) {
    Check.argNotNull(collection, "collection");
    Check.argNotNull(batchSize, "batchSize");
    Check.argNotNull(maxTimeMs, "maxTimeMs");

    this.cursorId = cursorId;
    this.collection = collection;
    this.batchSize = batchSize;
    this.maxTimeMs = maxTimeMs;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    BsonDocument doc =
        new BsonDocument()
            .append(GET_MORE_FIELD, new BsonInt64(this.cursorId))
            .append(COLLECTION_FIELD, new BsonString(this.collection));

    this.batchSize.ifPresent(batchSize -> doc.append(BATCH_SIZE_FIELD, new BsonInt32(batchSize)));
    this.maxTimeMs.ifPresent(maxTimeMs -> doc.append(MAX_TIME_MS_FIELD, new BsonInt32(maxTimeMs)));

    return doc;
  }
}
