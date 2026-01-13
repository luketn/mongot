package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.BsonUtils;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt64;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonNull;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.StringCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OptionalCodecTest {

  private static final Codec<String> STRING_CODEC = new StringCodec();

  private BsonDocument document;
  private BsonWriter bsonWriter;
  private BsonReader bsonReader;

  @Before
  public void setup() {
    this.document = new BsonDocument();
  }

  private void setupEncode() {
    this.bsonWriter = new BsonDocumentWriter(this.document);
    this.bsonWriter.writeStartDocument();
    this.bsonWriter.writeName("key");
  }

  private void setupDecode() {
    this.bsonReader = this.document.asBsonReader();
    this.bsonReader.readStartDocument();
    this.bsonReader.readName("key");
  }

  @Test
  public void testDecodeWithCorrectType() {
    Optional<String> expected = Optional.of("simple");
    Codec<Optional<String>> codec = new OptionalCodec(expected.getClass(), STRING_CODEC);

    this.document.append("key", new BsonString("simple"));
    setupDecode();
    Optional<String> result = codec.decode(this.bsonReader, DecoderContext.builder().build());
    Assert.assertNotNull("should decode Optional<String>", result);
    Assert.assertFalse("should not be empty", result.isEmpty());
    Assert.assertEquals("should have same value as expected", expected.get(), result.get());
  }

  @Test
  public void testDecodeWithNull() {
    Optional<String> expected = Optional.empty();
    Codec<Optional<String>> codec = new OptionalCodec(expected.getClass(), STRING_CODEC);

    this.document.append("key", new BsonNull());
    setupDecode();
    Optional<String> result = codec.decode(this.bsonReader, DecoderContext.builder().build());
    Assert.assertNotNull("should decode null to Optional.empty()", result);
    Assert.assertTrue("should be empty", result.isEmpty());
  }

  @Test(expected = BsonInvalidOperationException.class)
  public void testDecodeWithIncorrectType() {
    Optional expected = Optional.of(42L);
    Codec<Optional> codec = new OptionalCodec(expected.getClass(), STRING_CODEC);

    this.document.append("key", new BsonInt64(42L));
    setupDecode();
    codec.decode(this.bsonReader, DecoderContext.builder().build());
  }

  @Test
  public void testEncodeWithCorrectType() {
    setupEncode();
    Optional<String> optionalString = Optional.of("simple");
    Codec<Optional<String>> codec = new OptionalCodec(optionalString.getClass(), STRING_CODEC);
    codec.encode(this.bsonWriter, optionalString, BsonUtils.DEFAULT_FAST_CONTEXT);

    BsonValue expected = new BsonString("simple");

    Assert.assertEquals("should encode Optional<String>", expected, this.document.get("key"));
  }

  @Test
  public void testEncodeEmpty() {
    setupEncode();
    Optional<String> optionalString = Optional.empty();
    Codec<Optional<String>> codec = new OptionalCodec(optionalString.getClass(), STRING_CODEC);
    codec.encode(this.bsonWriter, optionalString, BsonUtils.DEFAULT_FAST_CONTEXT);

    BsonValue expected = new BsonNull();

    Assert.assertEquals("should encode empty Optional<String>", expected, this.document.get("key"));
  }

  @Test
  public void testEncodeNull() {
    setupEncode();
    Optional<String> optionalString = Optional.of("asdf");
    Codec<Optional<String>> codec = new OptionalCodec(optionalString.getClass(), STRING_CODEC);
    codec.encode(this.bsonWriter, null, BsonUtils.DEFAULT_FAST_CONTEXT);

    BsonValue expected = new BsonNull();

    Assert.assertEquals("should encode null", expected, this.document.get("key"));
  }
}
