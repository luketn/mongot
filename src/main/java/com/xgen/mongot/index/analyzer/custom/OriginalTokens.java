package com.xgen.mongot.index.analyzer.custom;

/**
 * An option for the DaitchMokotoffSoundex and AsciiFolding token filters to specify whether to
 * include or omit the original tokens from the output of the token filter.
 */
public enum OriginalTokens {
  /** Include the original tokens with the encoded tokens in the output. */
  INCLUDE,

  /** Omit the original tokens and include only the encoded tokens in the output. */
  OMIT;
}
