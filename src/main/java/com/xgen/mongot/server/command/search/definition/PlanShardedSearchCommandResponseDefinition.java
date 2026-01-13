package com.xgen.mongot.server.command.search.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class PlanShardedSearchCommandResponseDefinition implements DocumentEncodable {
  public static class Fields {
    public static final Field.Required<Integer> OK =
        Field.builder("ok").intField().mustBeWithinBounds(Range.of(0, 1)).required();
  }

  public static final int OK = 1;
  public final ShardedSearchPlan shardedSearchPlan;

  PlanShardedSearchCommandResponseDefinition(ShardedSearchPlan shardedSearchPlan) {
    this.shardedSearchPlan = shardedSearchPlan;
  }

  public static PlanShardedSearchCommandResponseDefinition create(
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan shardedSearchPlan) {
    return new PlanShardedSearchCommandResponseDefinition(shardedSearchPlan);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument res = BsonDocumentBuilder.builder().field(Fields.OK, OK).build();
    res.putAll(this.shardedSearchPlan.toBson());

    return res;
  }

  public static class ShardedSearchPlan implements DocumentEncodable {
    public static class Fields {
      public static final Field.Required<List<BsonDocument>> META_PIPELINE =
          Field.builder("metaPipeline").documentField().asList().required();

      public static final Field.WithDefault<BsonDocument> SORT_SPEC =
          Field.builder("sortSpec").documentField().optional().withDefault(DEFAULT_SORT_SPEC);

      public static final Field.WithDefault<Integer> PROTOCOL_VERSION =
          Field.builder("protocolVersion")
              .intField()
              .optional()
              .withDefault(CURRENT_PROTOCOL_VERSION);
    }

    public static final int CURRENT_PROTOCOL_VERSION = 1;

    public static final BsonDocument DEFAULT_SORT_SPEC =
        new BsonDocument().append("$searchScore", new BsonInt32(-1));

    public final List<BsonDocument> metaPipeline;
    public final BsonDocument sortSpec;

    public ShardedSearchPlan(List<BsonDocument> metaPipeline, BsonDocument sortSpec) {
      this.metaPipeline = metaPipeline;
      this.sortSpec = sortSpec;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.META_PIPELINE, this.metaPipeline)
          .field(Fields.SORT_SPEC, this.sortSpec)
          .field(ShardedSearchPlan.Fields.PROTOCOL_VERSION, CURRENT_PROTOCOL_VERSION)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ShardedSearchPlan)) {
        return false;
      }
      ShardedSearchPlan that = (ShardedSearchPlan) o;
      return Objects.equals(this.metaPipeline, that.metaPipeline)
          && Objects.equals(this.sortSpec, that.sortSpec);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.metaPipeline, this.sortSpec);
    }
  }
}
