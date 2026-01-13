package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.NumberFacetFieldDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import java.util.Optional;

public class NumericFieldDefinitionBuilder {

  private Optional<NumericFieldOptions.Representation> representation = Optional.empty();
  private Optional<Boolean> indexIntegers = Optional.empty();
  private Optional<Boolean> indexDoubles = Optional.empty();

  public static NumericFieldDefinitionBuilder builder() {
    return new NumericFieldDefinitionBuilder();
  }

  public NumericFieldDefinitionBuilder representation(
      NumericFieldOptions.Representation representation) {
    this.representation = Optional.of(representation);
    return this;
  }

  public NumericFieldDefinitionBuilder indexIntegers(boolean indexShapes) {
    this.indexIntegers = Optional.of(indexShapes);
    return this;
  }

  public NumericFieldDefinitionBuilder indexDoubles(boolean indexDoubles) {
    this.indexDoubles = Optional.of(indexDoubles);
    return this;
  }

  public NumberFieldDefinition buildNumberField() {
    return new NumberFieldDefinition(
        new NumericFieldOptions(
            this.representation.orElse(NumericFieldOptions.Fields.REPRESENTATION.getDefaultValue()),
            this.indexDoubles.orElse(NumericFieldOptions.Fields.INDEX_DOUBLES.getDefaultValue()),
            this.indexIntegers.orElse(
                NumericFieldOptions.Fields.INDEX_INTEGERS.getDefaultValue())));
  }

  public NumberFacetFieldDefinition buildNumberFacetField() {
    return new NumberFacetFieldDefinition(
        new NumericFieldOptions(
            this.representation.orElse(NumericFieldOptions.Fields.REPRESENTATION.getDefaultValue()),
            this.indexDoubles.orElse(NumericFieldOptions.Fields.INDEX_DOUBLES.getDefaultValue()),
            this.indexIntegers.orElse(
                NumericFieldOptions.Fields.INDEX_INTEGERS.getDefaultValue())));
  }
}
