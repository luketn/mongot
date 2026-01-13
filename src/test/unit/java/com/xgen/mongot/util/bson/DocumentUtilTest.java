package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.json.JsonParseException;
import org.junit.Assert;
import org.junit.Test;

public class DocumentUtilTest {
  @Test
  public void testParseJsonFile() throws Exception {
    String path = "src/test/unit/resources/util/bson/documentUtilBasicData.json";
    BsonDocument bson = DocumentUtil.documentFromPath(Paths.get(path));

    BsonDocument bsonExpected =
        new BsonDocument(
                "comments",
                new BsonArray(List.of(new BsonDocument("name", new BsonString("jack")))))
            .append("_id", new BsonInt32(1234));

    Assert.assertEquals(bsonExpected, bson);
  }

  @Test
  public void testParseJsonSingleArray() throws Exception {
    String path = "src/test/unit/resources/util/bson/documentUtilBasicDataArray.json";
    List<BsonDocument> docs = DocumentUtil.documentsFromPath(Paths.get(path));
    Assert.assertEquals(1, docs.size());
    BsonDocument bson = docs.get(0);

    BsonDocument bsonExpected =
        new BsonDocument(
                "comments",
                new BsonArray(List.of(new BsonDocument("name", new BsonString("jack")))))
            .append("_id", new BsonInt32(1234));

    Assert.assertEquals(bsonExpected, bson);
  }

  @Test
  public void testParseJsonMultiArray() throws IOException, BsonParseException {
    String path = "src/test/unit/resources/util/bson/documentUtilMultiDataArray.json";
    List<BsonDocument> docs = DocumentUtil.documentsFromPath(Paths.get(path));
    Assert.assertEquals(2, docs.size());

    BsonDocument bsonFirst = docs.get(0);
    BsonDocument bsonFirstExpected =
        new BsonDocument(
                "comments",
                new BsonArray(List.of(new BsonDocument("name", new BsonString("jack")))))
            .append("_id", new BsonInt32(1234));

    Assert.assertEquals(bsonFirstExpected, bsonFirst);

    BsonDocument bsonSecond = docs.get(1);
    BsonDocument bsonSecondExpected =
        new BsonDocument(
                "comments",
                new BsonArray(List.of(new BsonDocument("name", new BsonString("jill")))))
            .append("_id", new BsonInt32(5678));

    Assert.assertEquals(bsonSecondExpected, bsonSecond);
  }

  @Test
  public void testStream() throws IOException, BsonParseException {
    String json =
        "{\n"
            + "  \"comments\": [\n"
            + "    {\n"
            + "      \"name\": \"jackत\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"_id\": 1234\n"
            + "}";

    // need to read in the document as part of an array
    // (in case there are multiple), but once parsed we
    // ignore the array
    ByteArrayInputStream stream =
        new ByteArrayInputStream(StandardCharsets.UTF_8.encode("[" + json + "]").array());

    List<BsonDocument> docs = DocumentUtil.parseStream(stream);
    Assert.assertEquals(1, docs.size());
    BsonDocument doc = docs.get(0);

    Assert.assertEquals(BsonDocument.parse(json), doc);
  }

  @Test
  public void testParseInvalidPath() {
    // Test parsing fails for documentsFromPath & documentsFromPath
    Assert.assertThrows(
        NoSuchFileException.class,
        () -> DocumentUtil.documentFromPath(Paths.get("fileDoesNotExist.json")));

    Assert.assertThrows(
        NoSuchFileException.class,
        () -> DocumentUtil.documentsFromPath(Paths.get("fileDoesNotExist.json")));
  }

  @Test
  public void testParseInvalidJson() {
    // Test parsing fails for documentsFromPath & documentsFromPath
    Assert.assertThrows(
        BsonParseException.class,
        () ->
            DocumentUtil.documentFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBadData.json")));

    Assert.assertThrows(
        JsonParseException.class,
        () ->
            DocumentUtil.documentsFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBadData.json")));
  }

  @Test
  public void testParseInvalidBsonFile() {
    // Test parsing fails for documentsFromPath & documentsFromPath
    Assert.assertThrows(
        BsonParseException.class,
        () ->
            DocumentUtil.documentFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBadBson.bson")));

    Assert.assertThrows(
        BsonParseException.class,
        () ->
            DocumentUtil.documentsFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBadBson.bson")));
  }

  @Test
  public void testParseNonJsonNonBsonFile() {
    // Test parsing fails for documentsFromPath & documentsFromPath
    Assert.assertThrows(
        IOException.class,
        () ->
            DocumentUtil.documentFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBasicData.txt")));

    Assert.assertThrows(
        IOException.class,
        () ->
            DocumentUtil.documentsFromPath(
                Paths.get("src/test/unit/resources/util/bson/documentUtilBasicData.txt")));
  }

  @Test
  public void testParseBsonFile() throws Exception {
    /*
     Test that reading from BSONs works properly by ingesting a json,
     converting to bson, writing to disk, reading in, and comparing
     final version to original.
    */
    String jsonPath = "src/test/unit/resources/util/bson/documentUtilBasicData.json";
    BsonDocument bsonOriginal = DocumentUtil.documentFromPath(Paths.get(jsonPath));
    String jsonOriginal = bsonOriginal.toJson();

    String bsonPath = "src/test/unit/resources/util/bson/documentUtilBasicData.bson";
    var bytes =
        new RawBsonDocument(bsonOriginal, BsonUtils.BSON_DOCUMENT_CODEC).getByteBuffer().array();
    FileUtils.atomicallyReplace(Paths.get(bsonPath), bytes);

    BsonDocument bsonFinal = DocumentUtil.documentFromPath(Paths.get(bsonPath));

    Assert.assertEquals(bsonOriginal, bsonFinal);
    Assert.assertEquals(jsonOriginal, bsonFinal.toJson());
  }
}
