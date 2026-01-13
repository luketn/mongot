package com.xgen.testing.mongot.index.lucene;

import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import org.bson.BsonValue;

public class LuceneIndexUtils {
  public static byte[] encodeDocumentId(BsonValue documentId) {
    return LuceneDocumentIdEncoder.encodeDocumentId(documentId);
  }
}
