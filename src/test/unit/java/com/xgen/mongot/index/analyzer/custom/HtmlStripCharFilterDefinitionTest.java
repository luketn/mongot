package com.xgen.mongot.index.analyzer.custom;

import com.google.errorprone.annotations.Var;
import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.CharFilterTestUtil;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class HtmlStripCharFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    List<Set<String>> tagSets = List.of(Set.of(), Set.of("a"), Set.of("a", "href"));

    for (var tags : tagSets) {
      @Var
      var charFilterDefinition = CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder();

      for (var tag : tags) {
        charFilterDefinition = charFilterDefinition.ignoredTag(tag);
      }

      CharFilterTestUtil.testCharFilterProducesChars(charFilterDefinition.build(), "", "");
    }
  }

  @Test
  public void testDoesNotModifyRegularCharacters() throws Exception {
    List<Set<String>> tagSets = List.of(Set.of(), Set.of("a"), Set.of("a", "href"));

    for (var tags : tagSets) {
      @Var
      var charFilterDefinition = CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder();

      for (var tag : tags) {
        charFilterDefinition = charFilterDefinition.ignoredTag(tag);
      }

      CharFilterTestUtil.testCharFilterProducesChars(
          charFilterDefinition.build(),
          "the lazy fox jumped over the dog or something.!?",
          "the lazy fox jumped over the dog or something.!?");
    }
  }

  @Test
  public void testDoesNotModifyTagLikeChars() throws Exception {
    List<Set<String>> tagSets = List.of(Set.of(), Set.of("a"), Set.of("a", "href"));

    for (var tags : tagSets) {
      @Var
      var charFilterDefinition = CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder();

      for (var tag : tags) {
        charFilterDefinition = charFilterDefinition.ignoredTag(tag);
      }

      CharFilterTestUtil.testCharFilterProducesChars(
          charFilterDefinition.build(), "h1 <h1 p <p a", "h1 <h1 p <p a");
    }
  }

  @Test
  public void testStripsTags() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().ignoredTag("h1").build(),
        "<body><h1>My First Heading</h1><p>My first paragraph.</p></body>",
        "\n<h1>My First Heading</h1>\nMy first paragraph.\n\n");
  }
}
