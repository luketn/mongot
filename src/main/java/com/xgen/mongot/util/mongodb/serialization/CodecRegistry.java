package com.xgen.mongot.util.mongodb.serialization;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.xgen.mongot.util.bson.OptionalPropertyCodecProvider;
import java.util.Collections;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.Conventions;
import org.bson.codecs.pojo.PojoCodecProvider;

public class CodecRegistry {

  private static final String PACKAGE = CodecRegistry.class.getPackageName();

  public static final org.bson.codecs.configuration.CodecRegistry PACKAGE_CODEC_REGISTRY =
      CodecRegistries.fromRegistries(
          CodecRegistries.fromCodecs(
              ChangeStreamDocument.createCodec(
                  RawBsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())),
          MongoClientSettings.getDefaultCodecRegistry(),
          CodecRegistries.fromProviders(
              PojoCodecProvider.builder()
                  .conventions(Collections.singletonList(Conventions.ANNOTATION_CONVENTION))
                  .register(PACKAGE)
                  .register(new OptionalPropertyCodecProvider())
                  .build()));
}
