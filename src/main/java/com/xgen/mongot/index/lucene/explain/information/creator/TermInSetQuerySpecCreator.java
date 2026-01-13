package com.xgen.mongot.index.lucene.explain.information.creator;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.information.TermInSetQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;

public class TermInSetQuerySpecCreator {
  static TermInSetQuerySpec fromQuery(TermInSetQuery q) {
    return new TermInSetQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(q.getField())),
        getTermListFromQuery(q));
  }

  private static List<String> getTermListFromQuery(TermInSetQuery q) {
    // TermInSetQuery::getTermData is deprecated, may be removed in future Lucene release
    var termIter = q.getTermData().iterator();
    @Var var term = termIter.next();
    List<String> termList = new ArrayList<>();
    while (term != null) {
      termList.add(Term.toString(term));
      term = termIter.next();
    }
    return termList;
  }
}
