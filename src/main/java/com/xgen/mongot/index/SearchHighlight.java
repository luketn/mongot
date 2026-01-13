package com.xgen.mongot.index;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public record SearchHighlight(float score, StringPath path, List<SearchHighlightText> texts)
    implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<StringPath> PATH =
        Field.builder("path").classField(StringPath::fromBson).required();

    private static final Field.Required<Float> SCORE =
        Field.builder("score").floatField().required();

    private static final Field.Required<List<SearchHighlightText>> TEXTS =
        Field.builder("texts")
            .classField(SearchHighlightText::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();
  }

  public SearchHighlight {
    if (Float.isNaN(score)) {
      throw new IllegalArgumentException("score is NaN");
    }
  }

  static SearchHighlight fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchHighlight(
        parser.getField(Fields.SCORE).unwrap(),
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.TEXTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SCORE, this.score)
        .field(Fields.PATH, this.path)
        .field(Fields.TEXTS, this.texts)
        .build();
  }
}
