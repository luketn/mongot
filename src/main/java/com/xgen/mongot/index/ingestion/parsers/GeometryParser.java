package com.xgen.mongot.index.ingestion.parsers;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.geo.GeoJsonParser;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;

public class GeometryParser {

  private static final Codec<BsonDocument> DOCUMENT_CODEC = BsonUtils.BSON_DOCUMENT_CODEC;

  public static Optional<Geometry> parse(BsonReader reader) {
    try {
      try (var parser =
          BsonDocumentParser.fromRoot(
                  DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build()))
              .allowUnknownFields(true)
              .build()) {
        return Optional.of(GeoJsonParser.parseGeometry(parser));
      }
    } catch (BsonParseException unused) {
      // swallow exception when geo fails to parse
    }
    return Optional.empty();
  }
}
