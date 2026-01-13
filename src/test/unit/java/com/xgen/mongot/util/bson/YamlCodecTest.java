package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class YamlCodecTest {
  @Test
  public void testObjectId() throws Exception {
    String content = "groupId: 6378918402648841e5596970";
    BsonDocument result = YamlCodec.fromYaml(content);
    Assert.assertEquals("6378918402648841e5596970", result.getString("groupId").getValue());
  }

  @Test
  public void testFromYamlValid() throws Exception {
    Path path = Path.of("src/test/unit/resources/util/bson/yamlCodecValid.yaml");
    String content = Files.readString(path);
    BsonDocument result = YamlCodec.fromYaml(content);

    BsonDocument nestedDoc =
        new BsonDocument()
            .append("number", new BsonInt32(42))
            .append("string", new BsonString("foo"))
            .append("boolean", BsonBoolean.FALSE)
            .append("booleanLiteral", BsonBoolean.TRUE)
            .append("nullLiteral", BsonNull.VALUE)
            .append("numberWithUnderscores", new BsonInt32(42_000));
    BsonDocument expected =
        new BsonDocument()
            .append("nestedDoc", nestedDoc)
            .append("int64", new BsonInt64((long) 1 << 55))
            .append("theNaturalLogarithm", new BsonDouble(2.718281828459045))
            .append("multiLineString", new BsonString("hello\nworld\n"))
            .append(
                "unicodeString", new BsonString("1'; DROP TABLE users-- 1 (｡◕ ∀ ◕｡) 👩‍👧‍👦١٢٣"))
            .append(
                "arrayOfNumberAndString",
                new BsonArray(List.of(new BsonInt32(42), new BsonString("foo"))))
            .append("arrayOfOneEmptyObject", new BsonArray(List.of(new BsonDocument())));

    Assert.assertEquals(nestedDoc, result.getDocument("nestedDoc"));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testNonDocumentNullValueThrows() {
    String content = "null";
    Assert.assertThrows(BsonParseException.class, () -> YamlCodec.fromYaml(content));
  }

  @Test
  public void testNonDocumentNonNullValueThrows() {
    String content = "123";
    Assert.assertThrows(BsonParseException.class, () -> YamlCodec.fromYaml(content));
    String contentArray = "[1,2,\"foo\"]";
    Assert.assertThrows(BsonParseException.class, () -> YamlCodec.fromYaml(contentArray));
  }
}
