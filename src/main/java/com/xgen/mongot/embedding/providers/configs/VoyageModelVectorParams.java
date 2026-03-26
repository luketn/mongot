package com.xgen.mongot.embedding.providers.configs;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.TypeDescription;
import com.xgen.mongot.util.bson.parser.ValueEncoder;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * Vector index-related defaults carried in MMS {@code modelConfig} for Voyage. {@code
 * modelConfig.quantization} uses MMS conf-call literals ({@code float}, {@code scalar}, …), not
 * index-definition {@code none}. This type lives in the embedding package to avoid a Maven/Bazel
 * cycle with {@code index.definition}.
 */
public final class VoyageModelVectorParams {

  private VoyageModelVectorParams() {}

  /**
   * Similarity values for map entries in MMS {@code modelConfig.similarity} (keys are typically
   * per-quantization buckets from MMS). Wire format matches vector field {@code similarity}
   * (camelCase enum string).
   */
  public enum Similarity {
    EUCLIDEAN,
    DOT_PRODUCT,
    COSINE
  }

  /**
   * Quantization modes aligned with {@link com.xgen.mongot.index.definition.VectorQuantization}.
   * Full-precision vectors are {@code NONE}; MMS conf call sends wire {@code "float"} for that
   * case. {@code "float"} as a {@code modelConfig.similarity} map key is a bucket label, not this
   * enum.
   */
  public enum Quantization {
    NONE, // full-precision float vector
    SCALAR,
    BINARY,
    BINARY_NO_RESCORE
  }

  /**
   * MMS conf-call literals for {@code modelConfig.quantization} (exact match; {@code NONE} is wire
   * {@code float}, not {@code none}).
   */
  private static final ImmutableMap<String, Quantization> QUANTIZATION_BY_CONF_CALL_LITERAL =
      ImmutableMap.of(
          "float", Quantization.NONE,
          "scalar", Quantization.SCALAR,
          "binary", Quantization.BINARY,
          "binaryNoRescore", Quantization.BINARY_NO_RESCORE);

  /**
   * BSON encoder for {@code modelConfig.quantization}; matches {@link
   * #QUANTIZATION_BY_CONF_CALL_LITERAL}.
   */
  public static final ValueEncoder<Quantization> QUANTIZATION_MODEL_CONFIG_WIRE_ENCODER =
      quantization ->
          new BsonString(
              switch (quantization) {
                case NONE -> "float";
                case SCALAR -> "scalar";
                case BINARY -> "binary";
                case BINARY_NO_RESCORE -> "binaryNoRescore";
              });

  /** Parses {@code modelConfig.quantization} from conf-call string values. */
  public static Quantization parseQuantizationFromModelConfigWire(
      BsonParseContext context, BsonValue value) throws BsonParseException {
    if (value.getBsonType() != BsonType.STRING) {
      context.handleUnexpectedType(TypeDescription.STRING, value.getBsonType());
    }
    String wire = value.asString().getValue();
    Quantization quantization = QUANTIZATION_BY_CONF_CALL_LITERAL.get(wire);
    if (quantization == null) {
      return context.handleSemanticError("must be one of [float, scalar, binary, binaryNoRescore]");
    }
    return quantization;
  }

  /** Same values and BSON encoding as {@code indexingMethod} on vector field specifications. */
  public enum IndexingMethod {
    HNSW,
    FLAT
  }

  /**
   * HNSW graph parameters; same document shape as {@link
   * com.xgen.mongot.index.definition.VectorFieldSpecification.HnswOptions}.
   */
  public record HnswOptions(int maxEdges, int numEdgeCandidates) implements DocumentEncodable {

    public static final int MAXIMUM_MAX_EDGES = 64;
    public static final int DEFAULT_MAX_EDGES = 16;
    public static final int MAXIMUM_NUM_EDGE_CANDIDATES = 3200;
    public static final int DEFAULT_NUM_EDGE_CANDIDATES = 100;

    private static class Fields {

      private static final Field.WithDefault<Integer> MAX_EDGES =
          Field.builder("maxEdges")
              .intField()
              .mustBeWithinBounds(Range.of(DEFAULT_MAX_EDGES, MAXIMUM_MAX_EDGES))
              .optional()
              .withDefault(DEFAULT_MAX_EDGES);
      private static final Field.WithDefault<Integer> NUM_EDGE_CANDIDATES =
          Field.builder("numEdgeCandidates")
              .intField()
              .mustBeWithinBounds(
                  Range.of(DEFAULT_NUM_EDGE_CANDIDATES, MAXIMUM_NUM_EDGE_CANDIDATES))
              .optional()
              .withDefault(DEFAULT_NUM_EDGE_CANDIDATES);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .fieldOmitDefaultValue(Fields.MAX_EDGES, this.maxEdges)
          .fieldOmitDefaultValue(Fields.NUM_EDGE_CANDIDATES, this.numEdgeCandidates)
          .build();
    }

    public static HnswOptions fromBson(DocumentParser parser) throws BsonParseException {
      return new HnswOptions(
          parser.getField(Fields.MAX_EDGES).unwrap(),
          parser.getField(Fields.NUM_EDGE_CANDIDATES).unwrap());
    }
  }
}
