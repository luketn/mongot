package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SnowballStemmingTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    StemmingTestCase.builderWithIdentityMappingDefault(Collections.emptyList()).build().test();
  }

  @Test
  public void testUnmodified() throws Exception {
    // Georgian
    StemmingTestCase.builderWithIdentityMappingDefault(
            List.of("მინას", "ვჭამ", "და", "არა", "მტკივა."))
        .build()
        .test();

    // Hindi
    StemmingTestCase.builderWithIdentityMappingDefault(
            List.of(
                "मैं", "काँच", "खा", "सकता", "हूँ,", "मुझे", "उस", "से", "कोई", "पीडा", "नहीं",
                "होती."))
        .build()
        .test();

    // Hebrew
    StemmingTestCase.builderWithIdentityMappingDefault(
            List.of("אני", "יכול", "לאכול", "זכוכית", "וזה", "לא", "מזיק", "לי."))
        .build()
        .test();

    // Yiddish
    StemmingTestCase.builderWithIdentityMappingDefault(
            List.of("איך", "קען", "עסן", "גלאָז", "און", "עס", "טוט", "מיר", "נישט", "װײ."))
        .build()
        .test();

    // Greek
    StemmingTestCase.builderWithIdentityMappingDefault(
            List.of("Μπορώ", "να", "φάω", "σπασμένα", "γυαλιά", "χωρίς", "να", "πάθω", "τίποτα."))
        .build()
        .test();

    // Symbols
    StemmingTestCase.builderWithIdentityMappingDefault(List.of("€", "$")).build().test();
  }

  /**
   * Icelandic is a germanic language with frequent compound words. German, Dutch, Danish,
   * Norwegian, Swedish, and Finnish share this property.
   */
  @Test
  public void testIcelandic() throws Exception {
    var testCase =
        StemmingTestCase.builder(
            List.of("Ég", "get", "etið", "gler", "án", "þess", "að", "meiða", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN,
        List.of("Ég", "get", "etið", "gler", "án", "þess", "að", "meiða", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN2,
        List.of("Ég", "get", "etið", "gler", "án", "þess", "að", "meiða", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.DUTCH,
        List.of("Ég", "get", "etið", "gler", "an", "þes", "að", "meiða", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.DANISH,
        List.of("Ég", "get", "etið", "gler", "án", "þes", "að", "meiða", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.NORWEGIAN,
        List.of("Ég", "get", "etið", "gler", "án", "þess", "að", "meið", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.SWEDISH,
        List.of("Ég", "get", "etið", "gler", "án", "þess", "að", "meið", "mig"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.FINNISH,
        List.of("Ég", "get", "etið", "gler", "án", "þes", "að", "meiða", "mig"));

    testCase.build().test();
  }

  @Test
  public void testEnglish() throws Exception {
    var testCase =
        StemmingTestCase.builder(
            List.of(
                "Stemming",
                "is",
                "the",
                "process",
                "of",
                "producing",
                "morphological",
                "variants",
                "of",
                "a",
                "root",
                "base",
                "word"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.ENGLISH,
        List.of(
            "Stem",
            "is",
            "the",
            "process",
            "of",
            "produc",
            "morpholog",
            "variant",
            "of",
            "a",
            "root",
            "base",
            "word"));

    testCase.build().test();
  }

  @Test
  public void testEstonian() throws Exception {
    var testCase =
        StemmingTestCase.builder(
            List.of(
                "Üks", "loll", "võib", "rohkem", "küsida", "kui", "seitse", "tarka", "vastata"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.ESTONIAN,
        List.of("Üks", "loll", "võisi", "rohke", "küsi", "kui", "seitse", "tarka", "vasta"));

    testCase.build().test();
  }

  @Test
  public void testRomanian() throws Exception {
    var testCase =
        StemmingTestCase.builder(
            List.of("Pot", "să", "mănânc", "sticlă", "și", "ea", "nu", "mă", "rănește"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.ROMANIAN,
        List.of("Pot", "să", "mănânc", "sticl", "și", "ea", "nu", "mă", "răneșt"));

    testCase.build().test();
  }

  @Test
  public void testUkrainian() throws Exception {
    var testCase =
        StemmingTestCase.builderWithIdentityMappingDefault(
            List.of("Я", "можу", "їсти", "шкло,", "й", "воно", "мені", "не", "пошкодить"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.RUSSIAN,
        List.of("Я", "мож", "їсти", "шкло,", "й", "вон", "мені", "не", "пошкод"));

    testCase.build().test();
  }

  @Test
  public void testArabic() throws Exception {
    var testCase =
        StemmingTestCase.builder(
            List.of("أنا", "قادر", "على", "أكل", "الزجاج", "و", "هذا", "لا", "يؤلمني"));
    testCase.expect(
        SnowballStemmingTokenFilterDefinition.StemmerName.ARABIC,
        List.of("انا", "قادر", "علي", "اكل", "زجاج", "و", "هذا", "لا", "يولم"));

    testCase.build().test();
  }

  static class StemmingTestCase {
    static class Builder {
      final List<String> input;
      Map<SnowballStemmingTokenFilterDefinition.StemmerName, List<String>> expected;

      public Builder(List<String> input) {
        this.input = input;
        this.expected = new HashMap<>();
      }

      public Builder expect(
          SnowballStemmingTokenFilterDefinition.StemmerName stemmerName, List<String> expected) {
        this.expected.put(stemmerName, expected);
        return this;
      }

      public Builder unchangedWith(SnowballStemmingTokenFilterDefinition.StemmerName stemmerName) {
        return expect(stemmerName, this.input);
      }

      public StemmingTestCase build() {
        return new StemmingTestCase(this.input, this.expected);
      }
    }

    final List<String> input;
    final Map<SnowballStemmingTokenFilterDefinition.StemmerName, List<String>> expected;

    StemmingTestCase(
        List<String> input,
        Map<SnowballStemmingTokenFilterDefinition.StemmerName, List<String>> expected) {
      this.input = input;
      this.expected = expected;
    }

    public static Builder builder(List<String> input) {
      return new Builder(input);
    }

    public static Builder builderWithIdentityMappingDefault(List<String> input) {
      var builder = builder(input);
      Arrays.stream(SnowballStemmingTokenFilterDefinition.StemmerName.values())
          .forEach(builder::unchangedWith);
      return builder;
    }

    void test() throws Exception {
      for (var stemmingCase : this.expected.entrySet()) {
        TokenFilterTestUtil.testTokenFilterProducesTokens(
            TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
                .stemmerName(stemmingCase.getKey())
                .build(),
            this.input,
            stemmingCase.getValue(),
            String.format(
                "%s snowball stemmer should produce expected tokens", stemmingCase.getKey()));
      }
    }
  }
}
