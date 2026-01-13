package com.xgen.mongot.index;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.base.Objects;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * SearchSortValues are present per SearchResult generated from a sort operation. SearchSortValues
 * is a Map of all SortValues Lucene has used to determine the order of the corresponding
 * SearchResult.
 */
public final class SearchSortValues implements Encodable {
  static class Values {
    static final Value.Required<Map<String, SortValue>> SEARCH_SORT_VALUES =
        Value.builder()
            .mapOf(Value.builder().classValue(SortValue::fromBson).required())
            .validate(
                entry ->
                    entry.keySet().stream().allMatch(key -> key.matches(KEY_FORMAT_REGEX))
                        ? Optional.empty()
                        : Optional.of("Invalid $searchSortValues key format"))
            .required();
  }

  private final Map<String, SortValue> searchSortValues;

  private static final String KEY_FORMAT_REGEX = "^_\\d+$";

  private SearchSortValues(Map<String, SortValue> searchSortValues) {
    this.searchSortValues = searchSortValues;
  }

  public static SearchSortValues create(List<BsonValue> luceneSortValues) {
    Map<String, SortValue> searchSortValues = new LinkedHashMap<>();
    String keyFormat = "_%s";
    for (int i = 0; i < luceneSortValues.size(); i++) {
      SortValue value = SortValue.create(luceneSortValues.get(i));
      searchSortValues.put(String.format(keyFormat, i), value);
    }
    return new SearchSortValues(searchSortValues);
  }

  public static SearchSortValues fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return new SearchSortValues(Values.SEARCH_SORT_VALUES.getParser().parse(context, value));
  }

  @Override
  public BsonValue toBson() {
    return Values.SEARCH_SORT_VALUES.getEncoder().encode(this.searchSortValues);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchSortValues that = (SearchSortValues) o;
    return Objects.equal(this.searchSortValues, that.searchSortValues);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.searchSortValues);
  }

  private record SortValue(BsonValue value) implements Encodable {

    // At the time of writing, mongos does not know how to perform custom sort ordering for
    // nulls/missing/[] values (previously represented as BsonNull) when it merge sorts results from
    // different shards based off of BSON ordering. In order for mongot to support custom sort
    // ordering on sharded deployments given the current mongos implementation, we must map nulls to
    // BsonMinKey or BsonMaxKey in the meantime depending on whether nulls should be treated as
    // lowest or highest, respectively.
    //
    // TODO(CLOUDP-266197): add BsonType.NULL to this set and remove BsonType.MIN_KEY &
    // BsonType.MAX_KEY once mongos has completed its work to support custom reordering of BsonNulls
    // in SERVER-93244.
    private static final Set<BsonType> ALLOWED_TYPES =
        Set.of(
            BsonType.BINARY,
            BsonType.BOOLEAN,
            BsonType.DOUBLE,
            BsonType.DATE_TIME,
            BsonType.INT64,
            BsonType.MIN_KEY,
            BsonType.MAX_KEY,
            BsonType.OBJECT_ID,
            BsonType.STRING);

    static SortValue create(BsonValue luceneFieldValue) {
      checkArg(
          ALLOWED_TYPES.contains(luceneFieldValue.getBsonType()),
          "Unexpected lucene sort field value: %s",
          luceneFieldValue.getBsonType());
      return new SortValue(luceneFieldValue);
    }

    private static SortValue fromBson(BsonParseContext context, BsonValue value) {
      BsonType type = value.getBsonType();
      checkArg(
          ALLOWED_TYPES.contains(type), "value must be one of these types: [%s]", ALLOWED_TYPES);
      return new SortValue(value);
    }

    @Override
    public BsonValue toBson() {
      return this.value;
    }
  }
}
