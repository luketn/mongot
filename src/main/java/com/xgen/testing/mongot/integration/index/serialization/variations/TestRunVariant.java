package com.xgen.testing.mongot.integration.index.serialization.variations;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

public class TestRunVariant implements Encodable {

  public static class Fields {

    public static final Field.Required<TestType> TEST_TYPE =
        Field.builder("testType").enumField(TestType.class).asCamelCase().required();

    public static final Field.Optional<ShardZoneConfigName> SHARD_ZONE_CONFIG_NAME =
        Field.builder("shardZoneConfigName")
            .enumField(ShardZoneConfigName.class)
            .asCamelCase()
            .optional()
            .noDefault();

    public static final Field.Optional<SyncMode> SYNC_MODE =
        Field.builder("syncMode").enumField(SyncMode.class).asCamelCase().optional().noDefault();
  }

  private final TestType testType;
  private final Optional<ShardZoneConfigName> shardZoneConfigName;
  private final Optional<SyncMode> syncMode;

  public TestRunVariant(
      TestType testType,
      Optional<ShardZoneConfigName> shardZoneConfigName,
      Optional<SyncMode> syncMode) {
    this.testType = testType;
    this.shardZoneConfigName = shardZoneConfigName;
    this.syncMode = syncMode;
  }

  public static TestRunVariant fromBson(DocumentParser parser) throws BsonParseException {

    ParsedField.Required<TestType> testType = parser.getField(Fields.TEST_TYPE);
    ParsedField.Optional<ShardZoneConfigName> shardZoneConfigName =
        parser.getField(Fields.SHARD_ZONE_CONFIG_NAME);
    ParsedField.Optional<SyncMode> syncMode = parser.getField(Fields.SYNC_MODE);

    return new TestRunVariant(testType.unwrap(), shardZoneConfigName.unwrap(), syncMode.unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TEST_TYPE, this.testType)
        .field(Fields.SHARD_ZONE_CONFIG_NAME, this.shardZoneConfigName)
        .field(Fields.SYNC_MODE, this.syncMode)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestRunVariant that = (TestRunVariant) o;
    return Objects.equals(this.testType, that.testType)
        && Objects.equals(this.shardZoneConfigName, that.shardZoneConfigName)
        && Objects.equals(this.syncMode, that.syncMode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.testType, this.shardZoneConfigName, this.syncMode);
  }

  public TestType getTestType() {
    return this.testType;
  }

  public Optional<ShardZoneConfigName> getShardZoneConfigName() {
    return this.shardZoneConfigName;
  }

  public Optional<SyncMode> getSyncMode() {
    return this.syncMode;
  }
}
