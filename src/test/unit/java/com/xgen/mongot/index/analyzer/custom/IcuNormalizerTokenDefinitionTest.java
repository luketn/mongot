package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class IcuNormalizerTokenDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testNfc() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
            .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC)
            .build();

    // Tests the difference between modes, based on figure 6 here
    // https://unicode.org/reports/tr15/#Norm_Forms
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x1e9b) + Character.toString(0x0323)),
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x1e9b) + Character.toString(0x0323)));
  }

  @Test
  public void testNfd() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
            .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFD)
            .build();

    // Tests the difference between modes, based on figure 6 here
    // https://unicode.org/reports/tr15/#Norm_Forms
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x1e9b) + Character.toString(0x0323)),
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x017f) + Character.toString(0x0323) + Character.toString(0x0307)));
  }

  @Test
  public void testNfkc() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
            .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKC)
            .build();

    // Tests the difference between modes, based on figure 6 here
    // https://unicode.org/reports/tr15/#Norm_Forms
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x1e9b) + Character.toString(0x0323)),
        List.of(
            Character.toString(0x0066) + Character.toString(0x0069),
            Character.toString(0x0032) + Character.toString(0x0035),
            Character.toString(0x1e69)));
  }

  @Test
  public void testNfkd() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
            .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKD)
            .build();

    // Tests the difference between modes, based on figure 6 here
    // https://unicode.org/reports/tr15/#Norm_Forms
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            Character.toString(0xfb01),
            Character.toString(0x0032) + Character.toString(0x2075),
            Character.toString(0x1e9b) + Character.toString(0x0323)),
        List.of(
            Character.toString(0x0066) + Character.toString(0x0069),
            Character.toString(0x0032) + Character.toString(0x0035),
            Character.toString(0x0073) + Character.toString(0x0323) + Character.toString(0x0307)));
  }
}
