package com.xgen.mongot.util.bson.parser;

import java.util.Optional;

@FunctionalInterface
public interface FieldValidator<T> {

  Optional<String> validate(T t);
}
