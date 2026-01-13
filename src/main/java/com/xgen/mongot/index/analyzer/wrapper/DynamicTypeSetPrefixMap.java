package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

class DynamicTypeSetPrefixMap<T> {
  private final ImmutableMap<String, T> prefixToAnalyzer;

  DynamicTypeSetPrefixMap(ImmutableMap<String, T> prefixToAnalyzer) {
    this.prefixToAnalyzer = prefixToAnalyzer;
  }

  /**
   * For a given field name of the following data types `[token, string, autocomplete, multi]`, we
   * will traverse up a FieldPath to find the nearest matching analyzer. (Ex. luceneFieldName:
   * $type:token/field0.a, we will check if $type:token/field0.a, $type:token/field0, $type:token/
   * are in the prefix map with a mapped analyzer)
   *
   * @param luceneFieldName field name for which to get analyzer for
   * @return analyzer for given luceneFieldName
   */
  Optional<T> getNearestRoot(String luceneFieldName) {
    // TODO(CLOUDP-331934): Optimize this method
    validateFieldNameType(luceneFieldName);
    Optional<StringMultiFieldPath> maybeMultiFieldPath =
        FieldName.MultiField.isTypeOf(luceneFieldName)
            ? Optional.of(FieldName.MultiField.getFieldPath(luceneFieldName))
            : Optional.empty();
    String multiSuffix = maybeMultiFieldPath.map(x -> "." + x.getMulti()).orElse("");

    // get field path from either multi or luceneFieldName, then eagerly get parent as
    // this is a prefix map so we don't want to return exact matches
    @Var
    Optional<FieldPath> maybeFieldPath =
        maybeMultiFieldPath
            .map(StringMultiFieldPath::getFieldPath)
            .or(() -> Optional.ofNullable(FieldName.getFieldPath(luceneFieldName)))
            .flatMap(FieldPath::getParent);
    String fieldNamePrefix =
        FieldName.getPrefixFromLuceneFieldNameForTypeFieldOrMultiField(luceneFieldName);

    while (!FieldName.EmbeddedField.isAtEmbeddedRoot(maybeFieldPath).test(luceneFieldName)
        && maybeFieldPath.isPresent()) {
      String testLuceneFieldName =
          fieldNamePrefix + maybeFieldPath.orElse(FieldPath.parse("")) + multiSuffix;
      Optional<T> result = Optional.ofNullable(this.prefixToAnalyzer.get(testLuceneFieldName));
      if (result.isPresent()) {
        return result;
      }
      maybeFieldPath = maybeFieldPath.flatMap(FieldPath::getParent);
    }

    // We could still have a match at the embedded root so test that
    String testLuceneFieldName =
        fieldNamePrefix + maybeFieldPath.orElse(FieldPath.parse("")) + multiSuffix;
    T result = this.prefixToAnalyzer.get(testLuceneFieldName);
    return Optional.ofNullable(result);
  }

  private void validateFieldNameType(String luceneFieldName) {
    Check.checkArg(
        FieldName.TypeField.TOKEN.isTypeOf(luceneFieldName)
            || FieldName.TypeField.STRING.isTypeOf(luceneFieldName)
            || FieldName.TypeField.AUTOCOMPLETE.isTypeOf(luceneFieldName)
            || FieldName.MultiField.isTypeOf(luceneFieldName),
        "Field name is not of type [token, string, autocomplete, multi]: %s",
        luceneFieldName);
  }
}
