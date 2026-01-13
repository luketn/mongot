package com.xgen.mongot.util.bson;

import com.mongodb.MongoClientSettings;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

public class YamlCodec {

  private static final CodecRegistry CODEC_REGISTRIES =
      CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry());

  public static BsonDocument fromYaml(String yaml) throws BsonParseException {
    // parsed is a a tree of primitive values
    Map<?, ?> parsed = loadAsDocument(yaml);

    var doc = new BsonDocument();
    try (var writer = new BsonDocumentWriter(doc)) {
      CODEC_REGISTRIES.get(Map.class).encode(writer, parsed, BsonUtils.DEFAULT_FAST_CONTEXT);
    }
    return doc;
  }

  private static Map<?, ?> loadAsDocument(String yaml) throws BsonParseException {
    Object maybeDocument =
        new Yaml(
                new Constructor(new LoaderOptions()),
                new Representer(new DumperOptions()),
                new DumperOptions(),
                new CustomYamlResolver())
            .load(yaml);
    if (maybeDocument instanceof Map) {
      return (Map<?, ?>) maybeDocument;
    }
    throw new BsonParseException(
        String.format("no document at root level: %s", maybeDocument), Optional.empty());
  }

  /**
   * A customer resolver to always parse implicit {@code ObjectId}s as string types.
   *
   * <p>The default resolver may resolve {@code ObjectId}s as number types in edge cases.
   */
  private static class CustomYamlResolver extends Resolver {
    private static final Pattern OBJECT_ID_PATTERN = Pattern.compile("^[1234567890abcdef]{24}$");

    public CustomYamlResolver() {
      super();

      // Remove default implicit resolvers. So that the ObjectId pattern will have the highest
      // priority.
      this.yamlImplicitResolvers.clear();

      // Resolve ObjectId pattern as string types.
      addImplicitResolver(Tag.STR, OBJECT_ID_PATTERN, "1234567890abcdef");

      // Add back default implicit resolvers. So that it can resolve numbers, boolean, etc.
      addImplicitResolvers();
    }
  }
}
