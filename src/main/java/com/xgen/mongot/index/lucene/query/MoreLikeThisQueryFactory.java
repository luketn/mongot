package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.analyzer.wrapper.QueryAnalyzerWrapper;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoreLikeThisQueryFactory {
  private static class FieldAndText {
    final FieldPath path;
    final String text;

    public FieldAndText(FieldPath path, String text) {
      this.text = text;
      this.path = path;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(MoreLikeThisQueryFactory.class);

  private final SearchQueryFactoryContext context;
  private final QueryAnalyzerWrapper perFieldAnalyzer;

  MoreLikeThisQueryFactory(
      SearchQueryFactoryContext context, QueryAnalyzerWrapper perFieldAnalyzer) {
    this.context = context;
    this.perFieldAnalyzer = perFieldAnalyzer;
  }

  Query fromOperator(
      MoreLikeThisOperator moreLikeThisOperator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    var like = moreLikeThisOperator.like();

    List<FieldAndText> inputFieldAndTextPairs = new ArrayList<>();

    for (BsonDocument doc : like) {
      visitRootDocument(doc, inputFieldAndTextPairs);
    }

    // The fields that we'll be searching. This includes multis as well, so if the document is
    // {a: "hello"} and "a" is a multi field, we'll search in the multis too - that's why the
    // values in this map are a list.
    Map<FieldPath, List<String>> fieldToLuceneFieldName =
        getAllLuceneFieldNamesForPaths(inputFieldAndTextPairs);

    Map<String, Collection<Object>> moreLikeThisInput =
        getLuceneMoreLikeThisInput(inputFieldAndTextPairs, fieldToLuceneFieldName);

    try {
      MoreLikeThis moreLikeThis =
          createLuceneMoreLikeThis(singleQueryContext, fieldToLuceneFieldName);
      return moreLikeThis.like(moreLikeThisInput);
    } catch (IOException e) {
      LOG.error("Error while creating moreLikeThisQuery", e);
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private Map<String, Collection<Object>> getLuceneMoreLikeThisInput(
      List<FieldAndText> inputFieldAndTextPairs,
      Map<FieldPath, List<String>> fieldToAllLuceneNames) {

    Map<String, Collection<Object>> moreLikeThisInput = new HashMap<>();
    for (FieldAndText fieldAndText : inputFieldAndTextPairs) {
      if (fieldToAllLuceneNames.containsKey(fieldAndText.path)) {
        for (String luceneFieldName : fieldToAllLuceneNames.get(fieldAndText.path)) {
          moreLikeThisInput
              .computeIfAbsent(luceneFieldName, k -> new ArrayList<>())
              .add(fieldAndText.text);
        }
      }
    }
    return moreLikeThisInput;
  }

  /** Lucene's MoreLikeThis needs a specific setup, and we do that in this method. */
  private MoreLikeThis createLuceneMoreLikeThis(
      SingleQueryContext singleQueryContext, Map<FieldPath, List<String>> fieldToLuceneFieldName)
      throws IOException {

    String[] fieldNames =
        fieldToLuceneFieldName.values().stream().flatMap(Collection::stream).toArray(String[]::new);

    MoreLikeThis moreLikeThis = new MoreLikeThis(singleQueryContext.getIndexReader());

    // Lucene's default is 2, so if a word appears only once in "like", we'll ignore it, which is
    // a bad idea in practice. By setting to 0 we don't use minTermFreq at all.
    moreLikeThis.setMinTermFreq(0);

    // We want to search for a term only if it's present in at least two documents. Since MLT
    // is usually used by picking a document from a collection, every term will be present in at
    // least that document. If it's only present in the input document, it would be wasteful to
    // search based on it.
    moreLikeThis.setMinDocFreq(2);

    moreLikeThis.setFieldNames(fieldNames);
    moreLikeThis.setAnalyzer(this.perFieldAnalyzer);
    return moreLikeThis;
  }

  /**
   * Given a list of FieldPaths, we return the set all of indexed Lucene fields (including multis)
   * grouped by their corresponding FieldPath.
   */
  private Map<FieldPath, List<String>> getAllLuceneFieldNamesForPaths(List<FieldAndText> input) {

    Map<FieldPath, List<String>> fieldToAllLuceneFields = new HashMap<>();

    for (FieldAndText fieldAndText : input) {
      var stringFieldPath = new StringFieldPath(fieldAndText.path);
      // No point in searching in fields that are not indexed.
      if (!this.context
          .getQueryTimeMappingChecks()
          .isStringFieldIndexed(stringFieldPath, Optional.empty())) {
        continue;
      }

      if (!fieldToAllLuceneFields.containsKey(fieldAndText.path)) {
        // We're searching with all possible analyzers for a field as that might lead to better
        // results.
        // TODO(CLOUDP-123402): Figure out how this works for embedded.
        fieldToAllLuceneFields.put(
            fieldAndText.path,
            this.context.getAllLuceneFieldNames(stringFieldPath, Optional.empty()));
      }
    }
    return fieldToAllLuceneFields;
  }

  /**
   * Recursively traverses a BSON document to extract fields to be searched.
   *
   * @param doc The document.
   * @param fieldsToSearch We put the fields we'll search in here.
   */
  private void visitRootDocument(BsonDocument doc, List<FieldAndText> fieldsToSearch)
      throws InvalidQueryException {
    for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
      visitValue(entry.getValue(), FieldPath.newRoot(entry.getKey()), fieldsToSearch);
    }
  }

  private void visitDocument(BsonDocument doc, FieldPath path, List<FieldAndText> fieldsToSearch)
      throws InvalidQueryException {
    for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
      visitValue(entry.getValue(), path.newChild(entry.getKey()), fieldsToSearch);
    }
  }

  private void visitValue(BsonValue value, FieldPath path, List<FieldAndText> fieldsToSearch)
      throws InvalidQueryException {
    if (value.getBsonType() == BsonType.STRING) {
      visitString(value.asString(), path, fieldsToSearch);
    } else if (value.getBsonType() == BsonType.DOCUMENT) {
      visitDocument(value.asDocument(), path, fieldsToSearch);
    } else if (value.getBsonType() == BsonType.ARRAY) {
      for (BsonValue bsonValue : value.asArray()) {
        visitValue(bsonValue, path, fieldsToSearch);
      }
    }
  }

  private void visitString(
      BsonString bsonString, FieldPath path, List<FieldAndText> fieldsToSearch) {
    fieldsToSearch.add(new FieldAndText(path, bsonString.getValue()));
  }
}
