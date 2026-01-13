package com.xgen.mongot.index.lucene.util;

import static com.xgen.mongot.index.lucene.field.FieldName.getLuceneFieldNameForStringPath;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class AnalyzedText {

  /**
   * Analyzed tokens from text. The tokens can be used to build individual lucene.index.Term()s and
   * then compose complex lucene queries.
   */
  public static List<String> applyAnalyzer(
      Analyzer analyzer, StringPath stringPath, String text, Optional<FieldPath> embeddedRoot)
      throws IOException {

    List<String> terms = new ArrayList<>();

    // we wrap TokenStream in a try block in order for it be to closed automatically
    try (TokenStream source =
        analyzer.tokenStream(getLuceneFieldNameForStringPath(stringPath, embeddedRoot), text)) {
      CharTermAttribute charTermAttribute = source.addAttribute(CharTermAttribute.class);
      source.reset();
      while (source.incrementToken()) {
        terms.add(charTermAttribute.toString());
      }
    }
    return terms;
  }

  /**
   * Analyzed tokens from a collection of texts. The tokens can be used to build many
   * lucene.index.Terms at once for composing complex queries.
   */
  public static List<String> applyAnalyzer(
      Analyzer analyzer,
      StringPath pathOptionDefinition,
      Collection<String> texts,
      Optional<FieldPath> embeddedRoot)
      throws IOException {
    return CheckedStream.from(texts)
        .mapAndCollectChecked(tokenizeWith(analyzer, pathOptionDefinition, embeddedRoot))
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Applies analyzer which expects to return only a single token (normalizer). This is used for
   * index and query time processing of the token field type.
   */
  public static String applyTokenFieldTypeNormalizer(
      String luceneFieldName, Analyzer analyzer, String value) throws IOException {
    try (TokenStream stream = analyzer.tokenStream(luceneFieldName, value)) {
      CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
      stream.reset();

      if (!stream.incrementToken()) {
        throw new InvalidAnalyzerOutputException(
            "This analyzer is expected to produce exactly one token, but got none");
      }

      String result = charTermAttribute.toString();

      if (stream.incrementToken()) {
        throw new InvalidAnalyzerOutputException(
            "This analyzer is expected to produce exactly one token, but got many");
      }

      return result;
    } catch (NullPointerException e) {
      throw new InvalidAnalyzerOutputException("Analyzer is not found");
    }
  }

  private static CheckedFunction<String, List<String>, IOException> tokenizeWith(
      Analyzer analyzer, StringPath path, Optional<FieldPath> embeddedRoot) {
    return text -> applyAnalyzer(analyzer, path, text, embeddedRoot);
  }
}
