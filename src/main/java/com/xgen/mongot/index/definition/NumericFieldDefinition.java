package com.xgen.mongot.index.definition;

/** An interface for fields with numeric values to report their numeric options. */
public sealed interface NumericFieldDefinition
    permits NumberFacetFieldDefinition, NumberFieldDefinition {
  NumericFieldOptions options();

  boolean hasSameOptionsAs(NumericFieldDefinition other);
}
