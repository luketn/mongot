package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.ScoreDetails;
import com.xgen.mongot.util.Check;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ScoreDetailsBuilder {
  private Optional<Float> value = Optional.empty();
  private Optional<String> description = Optional.empty();
  private Optional<List<ScoreDetails>> details = Optional.empty();

  public static ScoreDetailsBuilder builder() {
    return new ScoreDetailsBuilder();
  }

  public ScoreDetailsBuilder value(float value) {
    this.value = Optional.of(value);
    return this;
  }

  public ScoreDetailsBuilder description(String description) {
    this.description = Optional.of(description);
    return this;
  }

  public ScoreDetailsBuilder details(List<ScoreDetails> scoreDetails) {
    this.details = Optional.of(scoreDetails);
    return this;
  }

  public ScoreDetails build() {
    Check.isPresent(this.value, "value");
    Check.isPresent(this.description, "description");
    return new ScoreDetails(
        this.value.get(), this.description.get(), this.details.orElse(Collections.emptyList()));
  }
}
