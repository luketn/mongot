package com.xgen.mongot.index.lucene.query.util;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class SynonymQueryUtil {

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static Query boostExactMatchSynonymQuery(
      Query synonymQuery, Query exactMatchQuery, int boostFactor) {
    // Boost exact match of a query by a constant value over synonym matches.
    // Do not boost if no synonyms are applied (Term/Phrase query is returned).
    // In the following explanations, we use an example boostFactor of 2.
    // Note that QueryBuilder in Lucene already applies a boost of 2.0 per exact query match. When
    // the exact match is one term, this was not enough to boost the result above longer synonym
    // matches, so we apply a custom boostFactor of 2 to the exact match query.
    //
    // Example 1:
    // Query: fast car
    // Synonyms: ["fast", "quick"]
    // In phrase queries, "fast car" is boosted by 4.0 (2.0 for the "fast" exact match applied by
    // QueryBuilder, 2.0 for
    // the "fast car" exact phrase match applied by mongot). "quick car" would have no boost.
    //
    // Example 2:
    // Query: fast car
    // Synonyms: ["fast", "quick"], ["car", "sedan"]
    // For phrase queries, "fast car" query would be boosted by 6.0 (2.0 by QueryBuilder for each
    // exact query match
    // "fast" and "car", and our custom boost to the exact phrase match of "fast car". "fast sedan"
    // and "quick car" would be boosted 2.0 by QueryBuilder for only one exact query match, and
    // "quick sedan" is not boosted.

    if (!(synonymQuery instanceof TermQuery || synonymQuery instanceof PhraseQuery)) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(synonymQuery, BooleanClause.Occur.SHOULD);
      builder.add(new BoostQuery(exactMatchQuery, boostFactor), BooleanClause.Occur.SHOULD);
      return builder.build();
    }
    return synonymQuery;
  }
}
