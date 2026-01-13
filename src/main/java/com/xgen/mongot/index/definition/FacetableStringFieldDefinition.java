package com.xgen.mongot.index.definition;

import java.util.Set;

public sealed interface FacetableStringFieldDefinition
    permits StringFacetFieldDefinition, TokenFieldDefinition {

  Set<FieldTypeDefinition.Type> TYPES =
      Set.of(FieldTypeDefinition.Type.TOKEN, FieldTypeDefinition.Type.STRING_FACET);

  FieldTypeDefinition.Type getType();
}
