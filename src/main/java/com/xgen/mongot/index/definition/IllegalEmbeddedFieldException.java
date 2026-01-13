package com.xgen.mongot.index.definition;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableException;
import java.util.Set;
import java.util.stream.Collectors;

public class IllegalEmbeddedFieldException extends LoggableException {

  private IllegalEmbeddedFieldException(String message) {
    super(message);
  }

  static IllegalEmbeddedFieldException withConflictingRelativeRoots(
      Set<FieldPath> conflictingRelativeRoots) {
    return new IllegalEmbeddedFieldException(
        String.format(
            "cannot define multiple embeddedDocuments fields at sub-paths %s",
            String.join(
                ", ",
                conflictingRelativeRoots.stream()
                    .map(FieldPath::toString)
                    .collect(Collectors.toSet()))));
  }

  static IllegalEmbeddedFieldException withIllegalFieldTypes(
      Set<FieldTypeDefinition.Type> illegalTypes) {
    return new IllegalEmbeddedFieldException(
        String.format(
            "cannot define fields of type %s inside an embeddedDocuments field",
            String.join(
                ", ",
                illegalTypes.stream()
                    .map(type -> Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type))
                    .collect(Collectors.toSet()))));
  }
}
