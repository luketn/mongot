package com.xgen.mongot.index.query.highlights;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public record UnresolvedHighlight(
    List<UnresolvedStringPath> paths, int maxNumPassages, int maxCharsToExamine)
    implements DocumentEncodable {

  public static class Fields {
    public static final Field.Required<List<UnresolvedStringPath>> PATH =
        Field.builder("path")
            .classField(UnresolvedStringPath::fromBson)
            .asSingleValueOrList()
            .mustNotBeEmpty()
            .required();

    /**
     * Specifies the number of highest-scoring passages to be returned per document when
     * highlighting per field. A passage is roughly the length of a sentence.
     */
    public static final Field.WithDefault<Integer> MAX_NUM_PASSAGES =
        Field.builder("maxNumPassages").intField().mustBePositive().optional().withDefault(5);

    /** The maximum number of characters to examine when performing highlighting a field. */
    public static final Field.WithDefault<Integer> MAX_CHARS_TO_EXAMINE =
        Field.builder("maxCharsToExamine")
            .intField()
            .mustBePositive()
            .optional()
            // Changed from Lucene's default value of 10000 to support highlighting larger fields.
            .withDefault(500000);
  }

  /** Deserializes an UnresolvedHighlight from the supplied DocumentParser. */
  public static UnresolvedHighlight fromBson(DocumentParser parser) throws BsonParseException {
    return new UnresolvedHighlight(
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.MAX_NUM_PASSAGES).unwrap(),
        parser.getField(Fields.MAX_CHARS_TO_EXAMINE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.paths)
        .field(Fields.MAX_NUM_PASSAGES, this.maxNumPassages)
        .field(Fields.MAX_CHARS_TO_EXAMINE, this.maxCharsToExamine)
        .build();
  }
}
