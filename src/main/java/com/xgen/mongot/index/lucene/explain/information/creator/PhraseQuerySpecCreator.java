package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.PhraseQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;

public class PhraseQuerySpecCreator {
  static PhraseQuerySpec fromQuery(org.apache.lucene.search.PhraseQuery query) {
    return new PhraseQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(query.getField())),
        queryStringFromTerms(query.getTerms()),
        query.getSlop());
  }

  private static String queryStringFromTerms(Term[] terms) {
    return String.format(
        "[%s]", Stream.of(terms).map(Term::text).collect(Collectors.joining(", ")));
  }
}
