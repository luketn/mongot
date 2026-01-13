package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.CharFilterTestUtil;
import org.junit.Test;

public class IcuNormalizeCharFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "", "");
  }

  @Test
  public void testDoesNotModifyRegularCharacters() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(
        charFilterDefinition,
        "the lazy fox jumped over the dog or something.!?",
        "the lazy fox jumped over the dog or something.!?");
  }

  @Test
  public void testNormalization() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(
        charFilterDefinition,
        "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि",
        "hピゴ5°cno(株)グラム,バッファーの正規化のテスト.アイウエオカキクケコザジズゼゾg̈각/각நிเกषिchkwक्षि");
  }
}
