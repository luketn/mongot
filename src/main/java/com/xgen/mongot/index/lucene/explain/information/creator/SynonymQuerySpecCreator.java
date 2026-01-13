package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.SynonymQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.index.Term;

public class SynonymQuerySpecCreator {
  static SynonymQuerySpec fromQuery(org.apache.lucene.search.SynonymQuery query) {
    List<Term> terms = query.getTerms();
    if (terms.isEmpty()) {
      return new SynonymQuerySpec(Optional.empty(), Collections.emptyList());
    }

    // field is the same for all terms in the SynonymQuery
    String field = terms.stream().findFirst().map(Term::field).get();
    List<String> values = terms.stream().map(Term::text).collect(Collectors.toList());

    FieldPath path = FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(field));
    return new SynonymQuerySpec(Optional.of(path), values);
  }
}
