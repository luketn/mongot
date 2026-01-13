package com.xgen.mongot.index.lucene.util;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.BsonUtils;
import java.nio.ByteBuffer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.BasicOutputBuffer;
import org.jetbrains.annotations.TestOnly;

public class LuceneDocumentIdEncoder {

  private static final BsonValueCodec CODEC = BsonUtils.BSON_VALUE_CODEC;

  /** The field where document IDs are stored in user documents. */
  private static final String MONGODB_DOCUMENT_ID_FIELD = "_id";

  private static final FieldType DOCUMENT_ID_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(true)
          .build();

  /** Returns the binary encoding of a BSON value. */
  public static byte[] encodeDocumentId(BsonValue documentId) {
    try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        // The BsonValueCodec will not allow you to directly encode a BsonValue, it must
        // be a Document. So for now we wrap the _id in a document by itself.
        // TODO(CLOUDP-280897): investigate above
        BsonDocument idDoc = new BsonDocument();
        idDoc.put(MONGODB_DOCUMENT_ID_FIELD, documentId);
        CODEC.encode(writer, idDoc, BsonUtils.DEFAULT_FAST_CONTEXT);

        // The BasicOutputBuffer starts with a 1024 byte buffer and will double its size if
        // needed: https://git.io/fhABL
        /// We only want to encode the bytes we actually want, so the encodedId byte array will just
        // be the bytes of the internal buffer that are actually being used.
        int size = buffer.size();
        byte[] id = new byte[size];
        System.arraycopy(buffer.getInternalBuffer(), 0, id, 0, size);

        return id;
      }
    }
  }

  /**
   * Decodes the binary encoded `_id` field from bytes that were encoded by encodeDocumentId.
   *
   * @param encodedDocumentId the binary encoded document ID
   * @return the decoded BsonValue representing the _id
   */
  public static BsonValue decodeDocumentId(byte[] encodedDocumentId) {
    ByteBuffer wrappedEncodedDocumentId = ByteBuffer.wrap(encodedDocumentId);

    BsonValue idValue;
    try (BsonReader reader = new BsonBinaryReader(wrappedEncodedDocumentId)) {
      // Have to read the BsonType so the decoder knows what kind of BsonValue to decode into.
      reader.readBsonType();
      idValue = CODEC.decode(reader, DecoderContext.builder().build());
    }

    if (!idValue.isDocument()) {
      // TODO(CLOUDP-280897): consider what this should actually throw
      throw new AssertionError(
          String.format(
              "found %s value that was not a BsonDocument",
              FieldName.MetaField.ID.getLuceneFieldName()));
    }

    BsonDocument idDoc = idValue.asDocument();
    if (!idDoc.containsKey(MONGODB_DOCUMENT_ID_FIELD)) {
      // TODO(CLOUDP-280897): consider what this should actually throw
      throw new AssertionError(
          String.format(
              "found %s document that did not contain %s key",
              FieldName.MetaField.ID.getLuceneFieldName(), MONGODB_DOCUMENT_ID_FIELD));
    }

    return idDoc.get(MONGODB_DOCUMENT_ID_FIELD);
  }

  /**
   * Retrieves the binary encoded `_id` field that was included in a Lucene document via
   * documentIdField, encoded by encodeDocumentId.
   */
  public static BsonValue documentIdFromLuceneDocument(Document document) {
    IndexableField idField = document.getField(FieldName.MetaField.ID.getLuceneFieldName());
    if (idField == null) {
      // TODO(CLOUDP-280897): consider what this should actually throw
      throw new AssertionError(
          String.format(
              "found document without %s field", FieldName.MetaField.ID.getLuceneFieldName()));
    }

    byte[] encodedDocumentId = idField.binaryValue().bytes;
    return decodeDocumentId(encodedDocumentId);
  }

  /** Returns the term for the `_id` included in the Lucene documents we index. */
  public static Term documentIdTerm(byte[] encodedDocumentId) {
    return new Term(FieldName.MetaField.ID.getLuceneFieldName(), new BytesRef(encodedDocumentId));
  }

  /**
   * Returns an IndexableField for the `_id` that should be included in the root Lucene document for
   * a BSON document we're indexing.
   */
  @TestOnly
  @VisibleForTesting
  public static IndexableField documentIdField(byte[] encodedDocumentId) {
    return new StoredField(
        FieldName.MetaField.ID.getLuceneFieldName(),
        new BytesRef(encodedDocumentId),
        DOCUMENT_ID_FIELD_TYPE);
  }
}
