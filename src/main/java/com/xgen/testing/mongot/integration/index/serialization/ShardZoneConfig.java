package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

public class ShardZoneConfig implements DocumentEncodable {
  public record ExplainOutput(
      Optional<SearchExplainInformation> zone0Explain,
      Optional<SearchExplainInformation> zone1Explain,
      Optional<SearchExplainInformation> zone2Explain)
      implements DocumentEncodable {
    public static class Fields {
      static final Field.Optional<SearchExplainInformation> ZONE_0 =
          Field.builder(ZONE_ZERO)
              .classField(SearchExplainInformation::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      static final Field.Optional<SearchExplainInformation> ZONE_1 =
          Field.builder(ZONE_ONE)
              .classField(SearchExplainInformation::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      static final Field.Optional<SearchExplainInformation> ZONE_2 =
          Field.builder(ZONE_TWO)
              .classField(SearchExplainInformation::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();
    }

    public static ExplainOutput fromRsToExplain(
        Map<String, SearchExplainInformation> perZoneExplain) {
      return new ExplainOutput(
          Optional.ofNullable(perZoneExplain.get(RS_ZERO)),
          Optional.ofNullable(perZoneExplain.get(RS_ONE)),
          Optional.ofNullable(perZoneExplain.get(RS_TWO)));
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ZONE_0, this.zone0Explain)
          .field(Fields.ZONE_1, this.zone1Explain)
          .field(Fields.ZONE_2, this.zone2Explain)
          .build();
    }

    public static ExplainOutput fromBson(DocumentParser parser) throws BsonParseException {
      return new ExplainOutput(
          parser.getField(Fields.ZONE_0).unwrap(),
          parser.getField(Fields.ZONE_1).unwrap(),
          parser.getField(Fields.ZONE_2).unwrap());
    }

    public List<Optional<SearchExplainInformation>> getAllZoneExplainInformation() {
      return List.of(this.zone0Explain, this.zone1Explain, this.zone2Explain);
    }
  }

  static class Fields {
    static final Field.Optional<List<ZoneRange>> ZONE_0 =
        Field.builder(ZONE_ZERO)
            .classField(ZoneRange::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<ZoneRange>> ZONE_1 =
        Field.builder(ZONE_ONE)
            .classField(ZoneRange::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<ZoneRange>> ZONE_2 =
        Field.builder(ZONE_TWO)
            .classField(ZoneRange::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  public static final String RS_ZERO = "rs0";
  public static final String RS_ONE = "rs1";
  public static final String RS_TWO = "rs2";

  public static final String ZONE_ZERO = "zone0";
  public static final String ZONE_ONE = "zone1";
  public static final String ZONE_TWO = "zone2";

  private final Optional<List<ZoneRange>> zone0;
  private final Optional<List<ZoneRange>> zone1;
  private final Optional<List<ZoneRange>> zone2;

  private ShardZoneConfig(
      Optional<List<ZoneRange>> zone0,
      Optional<List<ZoneRange>> zone1,
      Optional<List<ZoneRange>> zone2) {
    this.zone0 = zone0;
    this.zone1 = zone1;
    this.zone2 = zone2;
  }

  static ShardZoneConfig fromBson(DocumentParser parser) throws BsonParseException {
    parser
        .getGroup()
        .atLeastOneOf(
            parser.getField(Fields.ZONE_0),
            parser.getField(Fields.ZONE_1),
            parser.getField(Fields.ZONE_2));

    return new ShardZoneConfig(
        parser.getField(Fields.ZONE_0).unwrap(),
        parser.getField(Fields.ZONE_1).unwrap(),
        parser.getField(Fields.ZONE_2).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ZONE_0, this.zone0)
        .field(Fields.ZONE_1, this.zone1)
        .field(Fields.ZONE_2, this.zone2)
        .build();
  }

  public Map<String, Optional<List<ZoneRange>>> getZones() {
    return Map.of(RS_ZERO, this.zone0, RS_ONE, this.zone1, RS_TWO, this.zone2);
  }

  public Optional<List<ZoneRange>> getZone0() {
    return this.zone0;
  }

  public Optional<List<ZoneRange>> getZone1() {
    return this.zone1;
  }

  public Optional<List<ZoneRange>> getZone2() {
    return this.zone2;
  }
}
