package com.xgen.mongot.index.query;

import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;

public sealed interface Query extends DocumentEncodable permits SearchQuery, VectorSearchQuery {
  class Fields {
    public static final Field.WithDefault<String> INDEX =
        Field.builder("index").stringField().mustNotBeEmpty().optional().withDefault("default");

    public static final Field.Optional<SearchNodePreference> SEARCH_NODE_PREFERENCE =
        Field.builder("searchNodePreference")
            .classField(SearchNodePreference::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  String index();

  boolean concurrent();

  boolean returnStoredSource();

  /**
   * Specifies the path of the embedded root at which the query returns documents from. Must be used
   * with `returnStoredSource: true` unless `omitSearchDocumentResults: true` is set. Must refer to
   * a path indexed as `embeddedDocuments`. Not currently supported in $vectorSearch.
   *
   * @return returnScope path
   */
  Optional<ReturnScope> returnScope();
}
