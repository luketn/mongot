package com.xgen.mongot.index.analyzer.attributes;

/**
 * An analysis stage that changes a token stream. Input and output to/from a {@link
 * TokenStreamTransformationStage} is a {@link TokenStreamType}.
 */
public interface TokenStreamTransformationStage {
  /**
   * Given an input token stream of type {@link TokenStreamType}, determine what {@link
   * TokenStreamType} the output will be.
   */
  TokenStreamType outputTypeGiven(TokenStreamType inputType);

  /**
   * {@link TokenStreamTransformationStage}s that extend {@link OutputsSameTypeAsInput} do not
   * modify the token stream type; they output a token stream of the same type as they receive.
   */
  interface OutputsSameTypeAsInput extends TokenStreamTransformationStage {
    @Override
    default TokenStreamType outputTypeGiven(TokenStreamType tokenStreamType) {
      return tokenStreamType;
    }
  }

  /**
   * {@link TokenStreamTransformationStage}s that extend {@link AnyToGraph} produce {@code
   * TokenStreamType.GRAPH}-type token streams, regardless of the type of input token stream.
   */
  interface AnyToGraph extends TokenStreamTransformationStage {
    @Override
    default TokenStreamType outputTypeGiven(TokenStreamType unused) {
      return TokenStreamType.GRAPH;
    }
  }

  /**
   * {@link TokenStreamTransformationStage}s that extend {@link AnyToStream} produce {@code
   * TokenStreamType.STREAM}-type token streams, regardless of the type of input token stream.
   */
  interface AnyToStream extends TokenStreamTransformationStage {
    @Override
    default TokenStreamType outputTypeGiven(TokenStreamType unused) {
      return TokenStreamType.STREAM;
    }
  }
}
