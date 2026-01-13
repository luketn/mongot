package com.xgen.mongot.index.version;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public class Generation implements DocumentEncodable {
  static class Fields {
    static final Field.Required<Integer> USER_VERSION =
        Field.builder("userVersion").intField().mustBeNonNegative().required();
    static final Field.Required<Integer> FORMAT_VERSION =
        Field.builder("formatVersion").intField().mustBePositive().required();
    static final Field.WithDefault<Integer> ATTEMPT_NUMBER =
        Field.builder("attemptNumber").intField().mustBeNonNegative().optional().withDefault(0);
  }

  /** Generation assumed to all indexes defined before the introduction of no downtime indexing. */
  public static final Generation FIRST =
      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.FIVE);

  /** Generation of indexes created now. */
  public static final Generation CURRENT =
      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT);

  public final UserIndexVersion userIndexVersion;
  public final IndexFormatVersion indexFormatVersion;
  public final int attemptNumber;

  public Generation(
      UserIndexVersion userIndexVersion, IndexFormatVersion indexFormatVersion, int attemptNumber) {
    this.userIndexVersion = userIndexVersion;
    this.indexFormatVersion = indexFormatVersion;
    this.attemptNumber = attemptNumber;
  }

  public Generation(UserIndexVersion userIndexVersion, IndexFormatVersion indexFormatVersion) {
    this(userIndexVersion, indexFormatVersion, 0);
  }

  /** deserialize. */
  public static Generation fromBson(DocumentParser parser) throws BsonParseException {
    var user = new UserIndexVersion(parser.getField(Fields.USER_VERSION).unwrap());
    var backend = IndexFormatVersion.create(parser.getField(Fields.FORMAT_VERSION).unwrap());
    int attempt = parser.getField(Fields.ATTEMPT_NUMBER).unwrap();
    return new Generation(user, backend, attempt);
  }

  // This method should only be used in tests.
  @VisibleForTesting
  public Generation incrementUser() {
    return new Generation(this.userIndexVersion.increment(), this.indexFormatVersion);
  }

  public Generation nextAttempt() {
    return new Generation(this.userIndexVersion, this.indexFormatVersion, this.attemptNumber + 1);
  }

  public GenerationId generationId(ObjectId indexId) {
    return new GenerationId(indexId, this);
  }

  @Override
  public BsonDocument toBson() {
    var documentBuilder =
        BsonDocumentBuilder.builder()
            .field(Fields.USER_VERSION, this.userIndexVersion.versionNumber)
            .field(Fields.FORMAT_VERSION, this.indexFormatVersion.versionNumber);

    // Only serialize the attempt number if it is greater than zero, to maintain downgrade
    // compatibility with versions of mongot which cannot parse this field.
    if (this.attemptNumber > 0) {
      documentBuilder.field(Fields.ATTEMPT_NUMBER, this.attemptNumber);
    }

    return documentBuilder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Generation)) {
      return false;
    }
    Generation that = (Generation) o;
    return this.userIndexVersion.equals(that.userIndexVersion)
        && this.indexFormatVersion.equals(that.indexFormatVersion)
        && this.attemptNumber == that.attemptNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.userIndexVersion, this.indexFormatVersion, this.attemptNumber);
  }

  @Override
  public String toString() {
    return String.format(
        "Generation{user=%d, format=%d, attempt=%d}",
        this.userIndexVersion.versionNumber,
        this.indexFormatVersion.versionNumber,
        this.attemptNumber);
  }
}
