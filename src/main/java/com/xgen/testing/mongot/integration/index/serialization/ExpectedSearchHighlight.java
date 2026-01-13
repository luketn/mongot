package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public class ExpectedSearchHighlight implements DocumentEncodable {
  static class Fields {
    static final Field.Required<StringPath> PATH =
        Field.builder("path").classField(StringPath::fromBson).required();

    static final Field.Required<List<ExpectedSearchHighlightText>> TEXTS =
        Field.builder("texts")
            .classField(ExpectedSearchHighlightText::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final StringPath path;
  private final List<ExpectedSearchHighlightText> texts;

  private ExpectedSearchHighlight(StringPath path, List<ExpectedSearchHighlightText> texts) {
    this.path = path;
    this.texts = texts;
  }

  static ExpectedSearchHighlight fromBson(DocumentParser parser) throws BsonParseException {
    return new ExpectedSearchHighlight(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.TEXTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.TEXTS, this.texts)
        .build();
  }

  public StringPath getPath() {
    return this.path;
  }

  public List<ExpectedSearchHighlightText> getTexts() {
    return this.texts;
  }
}
