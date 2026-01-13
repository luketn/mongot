package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public abstract class CharFilterDefinition implements DocumentEncodable {
  static class Fields {
    static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  public enum Type {
    HTML_STRIP,
    ICU_NORMALIZE,
    MAPPING,
    PERSIAN
  }

  public abstract Type getType();

  abstract BsonDocument charFilterToBson();

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();

  @Override
  public final BsonDocument toBson() {
    BsonDocument doc = BsonDocumentBuilder.builder().field(Fields.TYPE, getType()).build();
    doc.putAll(charFilterToBson());
    return doc;
  }

  public static CharFilterDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case HTML_STRIP -> HtmlStripCharFilterDefinition.fromBson(parser);
      case ICU_NORMALIZE -> new IcuNormalizeCharFilterDefinition();
      case MAPPING -> MappingCharFilterDefinition.fromBson(parser);
      case PERSIAN -> new PersianCharFilterDefinition();
    };
  }

  public HtmlStripCharFilterDefinition asHtmlCharFilterDefinition() {
    Check.expectedType(Type.HTML_STRIP, this.getType());
    return (HtmlStripCharFilterDefinition) this;
  }

  public IcuNormalizeCharFilterDefinition asIcuNormalizeCharFilterDefinition() {
    Check.expectedType(Type.ICU_NORMALIZE, this.getType());
    return (IcuNormalizeCharFilterDefinition) this;
  }

  public MappingCharFilterDefinition asMappingCharFilterDefinition() {
    Check.expectedType(Type.MAPPING, this.getType());
    return (MappingCharFilterDefinition) this;
  }

  public PersianCharFilterDefinition asPersianCharFilterDefinition() {
    Check.expectedType(Type.PERSIAN, this.getType());
    return (PersianCharFilterDefinition) this;
  }
}
