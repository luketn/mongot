package com.xgen.mongot.server.command.management.definition.common;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinition.Type;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Arrays;
import java.util.Optional;

public sealed interface UserIndexDefinition extends DocumentEncodable
    permits UserSearchIndexDefinition, UserVectorIndexDefinition {

  static UserIndexDefinition fromBson(DocumentParser parser, IndexDefinition.Type type)
      throws BsonParseException {
    return type == Type.VECTOR_SEARCH
        ? UserVectorIndexDefinition.fromBson(parser)
        : UserSearchIndexDefinition.fromBson(parser);
  }

  int numPartitions();

  Optional<StoredSourceDefinition> storedSource();

  class Fields {
    public static final Field.WithDefault<Integer> NUM_PARTITIONS =
        Field.builder("numPartitions")
            .intField()
            .validate(
                num ->
                    Arrays.asList(1, 2, 4, 8, 16, 32, 64).contains(num)
                        ? Optional.empty()
                        : Optional.of(
                            String.format(
                                "numPartitions %d must be of 1, 2, 4, 8, 16, 32, or 64.", num)))
            .optional()
            .withDefault(1);
    public static final Field.Optional<StoredSourceDefinition> STORED_SOURCE =
        Field.builder("storedSource")
            .classField(StoredSourceDefinition::fromBson)
            .optional()
            .noDefault();
  }
}
