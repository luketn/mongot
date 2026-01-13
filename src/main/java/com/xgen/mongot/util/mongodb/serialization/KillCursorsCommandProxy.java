package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * KillCursorsCommandProxy is a proxy for a <em>killCursors</em> command.
 *
 * <p>See https://docs.mongodb.com/manual/reference/command/killCursors
 */
public class KillCursorsCommandProxy implements Bson {

  private static final String KILL_CURSORS_FIELD = "killCursors";
  private static final String CURSORS_FIELD = "cursors";

  private final String collection;
  private final List<Long> cursors;

  public KillCursorsCommandProxy(String collection, List<Long> cursors) {
    Check.argNotNull(collection, "collection");
    Check.argNotNull(cursors, "cursors");

    this.collection = collection;
    this.cursors = cursors;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    return new BsonDocument()
        .append(KILL_CURSORS_FIELD, new BsonString(this.collection))
        .append(
            CURSORS_FIELD,
            new BsonArray(this.cursors.stream().map(BsonInt64::new).collect(Collectors.toList())));
  }
}
