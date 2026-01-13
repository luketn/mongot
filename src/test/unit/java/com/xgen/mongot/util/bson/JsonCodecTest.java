package com.xgen.mongot.util.bson;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Arrays;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {JsonCodecTest.FromJson.class, JsonCodecTest.ToJson.class})
public class JsonCodecTest {

  public static class FromJson {
    @Test
    public void testSimpleFromJson() throws Exception {
      String json = "{\"a\": \"b\"}";
      BsonDocument bson = JsonCodec.fromJson(json);
      BsonDocument expected = new BsonDocument("a", new BsonString("b"));
      Assert.assertEquals(expected, bson);
    }

    @Test
    public void testInvalidFromJson() {
      String notJson = "this is not JSON";
      Exception ex = assertThrows(BsonParseException.class, () -> JsonCodec.fromJson(notJson));
      assertThat(ex).hasMessageThat().startsWith("Failed to parse");
      assertNotNull(ex.getCause());
    }
  }

  public static class ToJson {

    @Test
    public void testFiniteDouble() {
      BsonDocument bson = new BsonDocument("a", new BsonDouble(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13.0}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNanDouble() {
      BsonDocument bson = new BsonDocument("a", new BsonDouble(Double.NaN));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$numberDouble\": \"NaN\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testString() {
      BsonDocument bson = new BsonDocument("a", new BsonString("b"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"b\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDocument() {
      BsonDocument bson = new BsonDocument("a", new BsonDocument("b", new BsonBoolean(true)));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"b\": true}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testArray() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonArray(Arrays.asList(new BsonBoolean(true), new BsonBoolean(false))));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": [true, false]}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testUuidBinary() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonBinary(UUID.fromString("00000000-1111-2222-3333-444444444444")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"00000000-1111-2222-3333-444444444444\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNonUuidBinary() {
      byte[] bytes = new byte[] {0, 1, 2, 3};
      BsonDocument bson = new BsonDocument("a", new BsonBinary(bytes));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$binary\": {\"base64\": \"AAECAw==\", \"subType\": \"00\"}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testUndefined() {
      BsonDocument bson = new BsonDocument("a", new BsonUndefined());
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$undefined\": true}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testObjectId() {
      BsonDocument bson =
          new BsonDocument("a", new BsonObjectId(new ObjectId("012345678901234567890123")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"012345678901234567890123\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testBoolean() {
      BsonDocument bson = new BsonDocument("a", new BsonBoolean(true));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": true}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDateTime() {
      BsonDocument bson = new BsonDocument("a", new BsonDateTime(0));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$date\": \"1970-01-01T00:00:00Z\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNull() {
      BsonDocument bson = new BsonDocument("a", BsonNull.VALUE);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": null}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testRegularExpression() {
      BsonDocument bson = new BsonDocument("a", new BsonRegularExpression("$*^"));
      String json = JsonCodec.toJson(bson);
      String expected =
          "{\"a\": {\"$regularExpression\": {\"pattern\": \"$*^\", \"options\": \"\"}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDbPointer() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonDbPointer("foo.bar", new ObjectId("012345678901234567890123")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$ref\": \"foo.bar\", \"$id\": \"012345678901234567890123\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testJavascript() {
      BsonDocument bson = new BsonDocument("a", new BsonJavaScript("var a = undefined"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$code\": \"var a = undefined\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testSymbol() {
      BsonDocument bson = new BsonDocument("a", new BsonSymbol("my-symbol"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$symbol\": \"my-symbol\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testJavascriptWithScope() {
      BsonDocument bson =
          new BsonDocument(
              "a",
              new BsonJavaScriptWithScope(
                  "var a = undefined", new BsonDocument("b", new BsonBoolean(true))));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$code\": \"var a = undefined\", \"$scope\": {\"b\": true}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testInt32() {
      BsonDocument bson = new BsonDocument("a", new BsonInt32(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testTimestamp() {
      BsonDocument bson = new BsonDocument("a", new BsonTimestamp(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$timestamp\": {\"t\": 0, \"i\": 13}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testInt64() {
      BsonDocument bson = new BsonDocument("a", new BsonInt64(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDecimal128() {
      BsonDocument bson = new BsonDocument("a", new BsonDecimal128(new Decimal128(13)));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$numberDecimal\": \"13\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testMinKey() {
      BsonDocument bson = new BsonDocument("a", BsonUtils.MIN_KEY);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$minKey\": 1}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testMaxKey() {
      BsonDocument bson = new BsonDocument("a", BsonUtils.MAX_KEY);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$maxKey\": 1}}";
      Assert.assertEquals(expected, json);
    }
  }
}
