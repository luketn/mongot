package com.xgen.mongot.config.provider.community.parser;

import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.ValueEncoder;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.bson.BsonString;

public final class PathField {
  public static final ValueEncoder<Path> ENCODER = path -> new BsonString(path.toString());
  public static final ClassField.FromValueParser<Path> PARSER =
      (context, value) -> {
        if (!value.isString()) {
          context.handleUnexpectedType("string", value.getBsonType());
        }
        try {
          return Path.of(value.asString().getValue());
        } catch (InvalidPathException e) {
          return context.handleSemanticError("could not parse as path: " + e.getMessage());
        }
      };
}
