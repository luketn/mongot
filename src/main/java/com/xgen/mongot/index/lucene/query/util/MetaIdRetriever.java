package com.xgen.mongot.index.lucene.query.util;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.bson.BsonValue;

/** Utility class for the retrival of meta _id from storedFields. This class is not thread-safe */
public class MetaIdRetriever {
  private static final ImmutableSet<String> ID_FIELD =
      ImmutableSet.of(FieldName.MetaField.ID.getLuceneFieldName());

  private final StoredFields storedFields;

  public static MetaIdRetriever create(IndexReader reader) throws IOException {
    return new MetaIdRetriever(reader.storedFields());
  }

  private MetaIdRetriever(StoredFields storedFields) {
    this.storedFields = storedFields;
  }

  /**
   * Retrieves the meta _id from Lucene's stored fields.
   *
   * @param docId the Lucene docId from which to retrieve the meta _id.
   * @return the meta _id as a BsonValue, identical to the one in MongoD.
   * @throws IOException if Lucene encounters IO problems.
   */
  public BsonValue getRootMetaId(int docId) throws IOException {
    Document document = this.storedFields.document(docId, ID_FIELD);
    return LuceneDocumentIdEncoder.documentIdFromLuceneDocument(document);
  }
}
