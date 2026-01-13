package com.xgen.testing.mongot.index.query.collectors;

import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.Check;
import java.util.Map;
import java.util.Optional;

public class FacetCollectorBuilder extends CollectorBuilder {

  private Optional<Operator> operator;
  private Optional<Map<String, FacetDefinition>> facetDefinitions;
  private boolean withDrillSideways = false;

  public FacetCollectorBuilder operator(Operator operator) {
    this.operator = Optional.of(operator);
    return this;
  }

  public FacetCollectorBuilder facetDefinitions(Map<String, FacetDefinition> facetDefinitions) {
    this.facetDefinitions = Optional.of(facetDefinitions);
    return this;
  }

  public FacetCollectorBuilder withDrillSideways() {
    this.withDrillSideways = true;
    return this;
  }

  @Override
  public FacetCollector build() {
    Check.isPresent(this.operator, "operator");
    Check.isPresent(this.facetDefinitions, "facetDefinitions");
    return new FacetCollector(
        this.operator.get(), this.facetDefinitions.get(), getDrillSidewaysInfo());
  }

  private Optional<DrillSidewaysInfoBuilder.DrillSidewaysInfo> getDrillSidewaysInfo() {
    return this.withDrillSideways
        ? Optional.of(
            DrillSidewaysInfoBuilder.buildFacetOperators(
                this.operator.get(), this.facetDefinitions.get()))
        : Optional.empty();
  }
}
