package com.xgen.mongot.util.mongodb;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.conversions.Bson;

public class AggregationPipelineBuilder {

  private final List<Bson> pipeline;

  public AggregationPipelineBuilder() {
    this.pipeline = new ArrayList<>();
  }

  public AggregationPipelineBuilder addStage(Bson stage) {
    this.pipeline.add(stage);
    return this;
  }

  public AggregationPipelineBuilder addStage(Optional<Bson> stage) {
    stage.ifPresent(this::addStage);
    return this;
  }

  public AggregationPipelineBuilder addMultipleStages(List<Bson> stages) {
    stages.forEach(this::addStage);
    return this;
  }

  public AggregationPipelineBuilder addMultipleStages(Optional<List<Bson>> stages) {
    stages.ifPresent(this::addMultipleStages);
    return this;
  }

  public List<Bson> build() {
    return ImmutableList.copyOf(this.pipeline);
  }
}
