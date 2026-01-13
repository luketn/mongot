package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.MultiPhraseQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;

public class MultiPhraseQuerySpecCreator {
  static MultiPhraseQuerySpec fromQuery(org.apache.lucene.search.MultiPhraseQuery query) {
    String field =
        Stream.of(query.getTermArrays()).flatMap(Stream::of).findFirst().map(Term::field).get();
    return new MultiPhraseQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(field)),
        queryStringFromTerms(query.getTermArrays()),
        query.getSlop());
  }

  private static String queryStringFromTerms(Term[][] terms) {
    return String.format(
        "[%s]",
        Stream.of(terms)
            .map(posTerms -> Stream.of(posTerms).map(Term::text).collect(Collectors.joining("|")))
            .collect(Collectors.joining(", ")));
  }
}
