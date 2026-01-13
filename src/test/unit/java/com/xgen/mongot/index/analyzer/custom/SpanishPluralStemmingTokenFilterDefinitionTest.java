package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SpanishPluralStemmingTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build();

    List<String> regularWords = List.of("hola", "mi", "nombre", "es", "mongodb");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void testKeepsInvariants() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build();

    List<String> containsInvariants =
        List.of(
            "cosquillas",
            "quesadillas",
            "martes",
            "miercoles",
            "cumpleaños",
            "años",
            "novecientos",
            "baños",
            "virus",
            "tabús");
    List<String> expected =
        List.of(
            "cosquillas",
            "quesadilla",
            "martes",
            "miercol",
            "cumpleaños",
            "año",
            "novecientos",
            "baño",
            "virus",
            "tabu");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, containsInvariants, expected);
  }

  @Test
  public void testSpecialCases() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build();

    List<String> specialCases =
        List.of(
            "yoes",
            "noes",
            "sies",
            "clubes",
            "faralaes",
            "albalaes",
            "itemes",
            "albumes",
            "sandwiches",
            "relojes",
            "bojes",
            "contrarreloj",
            "carcajes");

    List<String> expectedSpecialCases =
        List.of(
            "yo",
            "no",
            "si",
            "club",
            "farala",
            "albala",
            "item",
            "album",
            "sandwich",
            "reloj",
            "boj",
            "contrarrel",
            "carcaj");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, specialCases, expectedSpecialCases);
  }

  @Test
  public void testRemoveAccents() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build();

    List<String> accents =
        List.of("sábado", "miércoles", "país", "celebración", "ambigüedad", "tú", "sueño", "PAÍS");
    List<String> removedAccents =
        List.of("sabado", "miercol", "pais", "celebracion", "ambiguedad", "tú", "sueño", "PAÍS");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, accents, removedAccents);
  }
}
