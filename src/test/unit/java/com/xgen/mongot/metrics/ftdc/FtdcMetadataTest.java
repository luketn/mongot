package com.xgen.mongot.metrics.ftdc;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class FtdcMetadataTest {
  @Test
  public void testEmpty() {
    FtdcMetadata result = new FtdcMetadata.Builder().build();
    BsonDocument expected = FtdcTestUtil.defaultMetadataDoc();

    FtdcTestUtil.assertMetadataDoc(
        "empty ftdc metadata should contain defaults", expected, result.serialize());
  }

  @Test
  public void testAddStaticInfo() {
    BsonString configPath = new BsonString("/some/path");
    FtdcMetadata result =
        new FtdcMetadata.Builder().addStaticInfo("configPath", configPath).build();

    BsonDocument expected = FtdcTestUtil.defaultMetadataDoc().append("configPath", configPath);

    FtdcTestUtil.assertMetadataDoc("staticInfo should be serialized", expected, result.serialize());
  }

  @Test
  public void testAddStaticInfoMultipleTimes() {
    BsonString configPath = new BsonString("/some/path");
    FtdcMetadata.Builder result =
        new FtdcMetadata.Builder().addStaticInfo("configPath", configPath);

    Assert.assertThrows(
        IllegalArgumentException.class, () -> result.addStaticInfo("configPath", configPath));
  }

  @Test
  public void testAddReservedKeyToStaticInfo() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new FtdcMetadata.Builder().addStaticInfo("type", new BsonString("elasticsearch")));
  }
}
