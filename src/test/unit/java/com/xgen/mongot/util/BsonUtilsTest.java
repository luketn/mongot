package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.bson.DocumentUtil;
import java.nio.file.Paths;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class BsonUtilsTest {

  @Test
  public void testIsOversized() throws Exception {
    BsonDocument smallBson =
        DocumentUtil.documentFromPath(
            Paths.get("src/test/unit/resources/util/documentUtilBasicData.json"));

    // This loop creates a ~17 MiB bson document.
    BsonDocument largeBson = new BsonDocument();
    for (int i = 0; i < 1_500_000; i++) {
      largeBson.append(String.valueOf(i), new BsonInt32(i));
    }

    Assert.assertFalse(BsonUtils.isOversized(smallBson));
    Assert.assertTrue(BsonUtils.isOversized(largeBson));
  }

  @Test
  public void emptyIsMinimal() {
    RawBsonDocument empty = BsonUtils.emptyDocument();
    RawBsonDocument canonical = RawBsonDocument.parse("{}");

    assertEquals(canonical, empty);
    assertEquals(canonical.getByteBuffer().remaining(), empty.getByteBuffer().array().length);
  }

  @Test
  public void emptyReturnsNewInstance() {
    RawBsonDocument modified = BsonUtils.emptyDocument();
    modified.getByteBuffer().put((byte) 42);
    RawBsonDocument empty = BsonUtils.emptyDocument();

    assertEquals(42, modified.getByteBuffer().get());
    assertEquals(5, empty.getByteBuffer().get());
  }
}
