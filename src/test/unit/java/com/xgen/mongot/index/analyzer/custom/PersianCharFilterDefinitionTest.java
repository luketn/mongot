package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.CharFilterTestUtil;
import org.junit.Test;

public class PersianCharFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.PersianCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "", "");
  }

  @Test
  public void testDoesNotModifyRegularCharacters() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.PersianCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(
        charFilterDefinition,
        "the lazy fox jumped over the dog or something.!?",
        "the lazy fox jumped over the dog or something.!?");
  }

  @Test
  public void testReplacesZeroWidthJoinerWithRegularSpace() throws Exception {
    var charFilterDefinition = CustomCharFilterDefinitionBuilder.PersianCharFilter.build();

    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "می‌خواهم", "می خواهم");
    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "הֱ‌ֽיֹות", "הֱ ֽיֹות");
    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "Auf‌lage", "Auf lage");
    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "Brot‌zeit", "Brot zeit");
    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "deaf‌ly", "deaf ly");
    CharFilterTestUtil.testCharFilterProducesChars(
        charFilterDefinition, "श्रीमान्‌को", "श्रीमान् को");
    CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition, "র‌্যাঁদা", "র ্যাঁদা");
  }
}
