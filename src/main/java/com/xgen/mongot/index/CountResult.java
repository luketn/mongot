package com.xgen.mongot.index;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

public class CountResult implements DocumentEncodable {

  private static class Fields {
    static final Field.Optional<Long> TOTAL =
        Field.builder("total").longField().optional().noDefault();

    static final Field.Optional<Long> LOWER_BOUND =
        Field.builder("lowerBound").longField().optional().noDefault();
  }

  private final Optional<Long> total;
  private final Optional<Long> lowerBound;

  CountResult(Optional<Long> total, Optional<Long> lowerBound) {
    if (total.isEmpty() && lowerBound.isEmpty()) {
      throw new IllegalArgumentException("CountResult needs at least one of total and lowerBound.");
    }
    this.total = total;
    this.lowerBound = lowerBound;
  }

  public static CountResult lowerBoundCount(long lowerBound) {
    return new CountResult(Optional.empty(), Optional.of(lowerBound));
  }

  public static CountResult totalCount(long total) {
    return new CountResult(Optional.of(total), Optional.empty());
  }

  /**
   * Merges a list of countResults in to one countResult. The resultant countResult is a total count
   * if and only if the list of countResults are all total count.
   */
  public static CountResult merge(List<CountResult> countResults) {
    long sum = countResults.stream().mapToLong(CountResult::getTotalOrLower).sum();
    boolean sumIsTotal =
        countResults.stream().allMatch(countResult -> countResult.getTotal().isPresent());
    return sumIsTotal ? totalCount(sum) : lowerBoundCount(sum);
  }

  public static CountResult fromBson(DocumentParser parser) throws BsonParseException {
    return new CountResult(
        parser.getField(Fields.TOTAL).unwrap(), parser.getField(Fields.LOWER_BOUND).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TOTAL, this.total)
        .field(Fields.LOWER_BOUND, this.lowerBound)
        .build();
  }

  public Optional<Long> getLowerBound() {
    return this.lowerBound;
  }

  public Optional<Long> getTotal() {
    return this.total;
  }

  /** Gets whichever is present between the total and lower count. */
  public Long getTotalOrLower() {
    checkState(
        this.total.isPresent() || this.lowerBound.isPresent(),
        "Count result has neither lower bound nor total count.");
    return this.total.orElseGet(this.lowerBound::get);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.total, this.lowerBound);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CountResult other = (CountResult) obj;
    return Objects.equals(this.total, other.total)
        && Objects.equals(this.lowerBound, other.lowerBound);
  }
}
