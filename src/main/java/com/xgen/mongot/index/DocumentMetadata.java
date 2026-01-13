package com.xgen.mongot.index;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

/** The contents of document metadata namespace. */
public class DocumentMetadata {

  private static final String ID_FIELD = "_id";
  private static final String DELETED_FIELD = "deleted";
  private static final boolean DELETED_DEFAULT = false;

  /**
   * The _id value extracted from the metadata namespace is guaranteed to be the original _id, while
   * _id from the root of the document might be modified by the aggregation pipeline configured for
   * a view. _id might not be present, which is expected for DELETE events and possible for UPDATE
   * events (that we treat as deletes).
   */
  private final Optional<BsonValue> id;

  /**
   * The deleted flag, populated as true if the user-defined $match stage in the view filters it
   * out. If the flag is not present, it's defaulted to false.
   */
  private final boolean deleted;

  /** A flag to keep track of the duplicate field issue (HELP-60413). */
  private final boolean metadataNamespaceIsPresent;

  private DocumentMetadata(
      Optional<BsonValue> id, boolean deleted, boolean metadataNamespaceIsPresent) {
    this.id = id;
    this.deleted = deleted;
    this.metadataNamespaceIsPresent = metadataNamespaceIsPresent;
  }

  public static DocumentMetadata fromMetadataNamespace(
      Optional<RawBsonDocument> document, ObjectId indexId) {

    Optional<BsonDocument> metadataNamespace =
        document.flatMap(
            doc -> Optional.ofNullable(doc.get(indexId.toString())).map(BsonValue::asDocument));

    Optional<BsonValue> id = metadataNamespace.map(DocumentMetadata::extractId);
    boolean deleted =
        metadataNamespace
            .map(
                namespace ->
                    namespace
                        .getBoolean(DELETED_FIELD, new BsonBoolean(DELETED_DEFAULT))
                        .getValue())
            .orElse(DELETED_DEFAULT);

    return new DocumentMetadata(id, deleted, metadataNamespace.isPresent());
  }

  /**
   * Constructor for documents that might not have valid metadata namespace present. Used as a
   * workaround for the case when user document contains duplicate fields, causing "undefined"
   * $project behavior on mongod side (HELP-60413). Should only be used when index is created on a
   * collection, not a view. Can be removed after SERVER-6439 is implemented.
   */
  public static DocumentMetadata fromOriginalDocument(Optional<RawBsonDocument> document) {
    Optional<BsonValue> id = document.map(DocumentMetadata::extractId);
    return new DocumentMetadata(id, DELETED_DEFAULT, false);
  }

  public static BsonValue extractId(BsonDocument document) {
    Check.isNotNull(document, "document");
    return document.get(ID_FIELD);
  }

  public Optional<BsonValue> getId() {
    return this.id;
  }

  public boolean isDeleted() {
    return this.deleted;
  }

  public boolean isMetadataNamespacePresent() {
    return this.metadataNamespaceIsPresent;
  }
}
