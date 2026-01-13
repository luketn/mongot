package com.xgen.mongot.index.lucene.commit;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent the commit data in Lucene. Currently, there are two schemas:
 *
 * <ul>
 *   <li>Old Schema - There is no {@link IndexWriterData}. {@link EncodedUserData} is stored as a
 *       map directly. e.g. {@code {"USER_DATA_Key1": ..., "USER_DATA_Key2": ..., ... }}
 *   <li>New Schema - {@link IndexWriterData} and {@link EncodedUserData} are stored as two map
 *       entries. {@link EncodedUserData} is stored as an encoded string. e.g. {@code
 *       {"indexWriterData": ..., "userData": "encodedString"}}
 * </ul>
 */
public class LuceneCommitData {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneCommitData.class);

  public static class Keys {
    public static final String INDEX_WRITER_DATA = "indexWriterData";
    public static final String USER_DATA = "userData";
  }

  public static class IndexWriterData {
    private static class Fields {
      private static final Field.Required<Boolean> IS_CLEARED =
          Field.builder("isCleared").booleanField().required();
    }

    public static final IndexWriterData EMPTY = new IndexWriterData(false);

    // Indicates whether the index data is cleared. If only a portion of index-partitions are
    // cleared, we may need to clear rest of them.
    private final boolean isCleared;

    public IndexWriterData(boolean isCleared) {
      this.isCleared = isCleared;
    }

    public boolean isCleared() {
      return this.isCleared;
    }

    public static IndexWriterData fromEncodedString(String encodedString)
        throws BsonParseException {
      BsonDocument bsonDocument = JsonCodec.fromJson(encodedString);
      try (var parser = BsonDocumentParser.fromRoot(bsonDocument).build()) {
        return new IndexWriterData(parser.getField(Fields.IS_CLEARED).unwrap());
      }
    }

    public String toEncodedString() {
      return BsonDocumentBuilder.builder()
          .field(Fields.IS_CLEARED, this.isCleared)
          .build()
          .toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IndexWriterData that = (IndexWriterData) o;
      return this.isCleared == that.isCleared;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.isCleared);
    }
  }

  private final IndexWriterData indexWriterData;
  private final EncodedUserData encodedUserData;

  public LuceneCommitData(IndexWriterData indexWriterData, EncodedUserData encodedUserData) {
    this.indexWriterData = indexWriterData;
    this.encodedUserData = encodedUserData;
  }

  public IndexWriterData getIndexWriterData() {
    return this.indexWriterData;
  }

  public EncodedUserData getEncodedUserData() {
    return this.encodedUserData;
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static LuceneCommitData fromDataMap(
      Optional<Iterable<Map.Entry<String, String>>> optionalDataMap) {
    Map<String, String> dataMap = new HashMap<>();
    optionalDataMap.ifPresent(
        iterable -> iterable.forEach(entry -> dataMap.put(entry.getKey(), entry.getValue())));
    if (dataMap.isEmpty()) {
      return new LuceneCommitData(IndexWriterData.EMPTY, EncodedUserData.EMPTY);
    }
    if (!dataMap.containsKey(Keys.USER_DATA)) {
      LOG.error("Cannot find userData. Will return empty LuceneCommitData.");
      return new LuceneCommitData(IndexWriterData.EMPTY, EncodedUserData.EMPTY);
    }

    @Var IndexWriterData indexWriterData = IndexWriterData.EMPTY;
    if (dataMap.containsKey(Keys.INDEX_WRITER_DATA)) {
      try {
        indexWriterData = IndexWriterData.fromEncodedString(dataMap.get(Keys.INDEX_WRITER_DATA));
      } catch (BsonParseException e) {
        // If we failed to parse IndexWriterData, returns empty LuceneCommitData to trigger
        // re-sync.
        LOG.atError()
            .setCause(e)
            .addKeyValue("dataMap", dataMap)
            .log("Cannot parse indexWriterData. Will return empty LuceneCommitData.");
        return new LuceneCommitData(IndexWriterData.EMPTY, EncodedUserData.EMPTY);
      }
    }
    return new LuceneCommitData(
        indexWriterData, EncodedUserData.fromString(dataMap.get(Keys.USER_DATA)));
  }

  public Iterable<Map.Entry<String, String>> toDataMapEntries() {
    return Map.of(
            Keys.INDEX_WRITER_DATA,
            this.indexWriterData.toEncodedString(),
            Keys.USER_DATA,
            this.encodedUserData.asString())
        .entrySet();
  }
}
