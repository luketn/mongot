package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * ChangeStreamPipelineStageOptionsProxy is a proxy for the options that may be passed in to a
 * $changeStream aggregation pipeline stage.
 *
 * <p>See
 * https://github.com/mongodb/mongo/blob/72788deb99186ac2628604505147e68ac53cffc5/src/mongo/db/pipeline/document_source_change_stream.idl
 */
public class ChangeStreamPipelineStageOptionsProxy implements Bson {

  public static final String FULL_DOCUMENT_UPDATE_LOOKUP = "updateLookup";

  private static final String START_AFTER_FIELD = "startAfter";
  private static final String START_AT_OPERATION_TIME_FIELD = "startAtOperationTime";
  private static final String FULL_DOCUMENT_FIELD = "fullDocument";
  private static final String SHOW_MIGRATION_EVENTS_FIELD = "showMigrationEvents";
  private static final String SHOW_EXPANDED_EVENTS_FIELD = "showExpandedEvents";
  private static final String MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP =
      "matchCollectionUUIDForUpdateLookup";

  private final Optional<BsonDocument> startAfter;
  private final Optional<BsonTimestamp> startAtOperationTime;
  private final Optional<String> fullDocument;
  private final Optional<Boolean> showMigrationEvents;
  private final Optional<Boolean> showExpandedEvents;
  private final Optional<Boolean> matchCollectionUuidForUpdateLookup;

  /** Constructs a ChangeStreamPipelineStageOptionsProxy. */
  public ChangeStreamPipelineStageOptionsProxy(
      Optional<BsonDocument> startAfter,
      Optional<BsonTimestamp> startAtOperationTime,
      Optional<String> fullDocument,
      Optional<Boolean> showMigrationEvents,
      Optional<Boolean> showExpandedEvents,
      Optional<Boolean> matchCollectionUuidForUpdateLookup) {
    Check.argNotNull(startAfter, "startAfter");
    Check.argNotNull(startAtOperationTime, "startAtOperationTime");
    Check.argNotNull(fullDocument, "fullDocument");
    Check.argNotNull(showMigrationEvents, "showMigrationEvents");
    Check.argNotNull(showExpandedEvents, "showExpandedEvents");
    Check.argNotNull(matchCollectionUuidForUpdateLookup, "matchCollectionUuidForUpdateLookup");

    this.startAfter = startAfter;
    this.startAtOperationTime = startAtOperationTime;
    this.fullDocument = fullDocument;
    this.showMigrationEvents = showMigrationEvents;
    this.showExpandedEvents = showExpandedEvents;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    BsonDocument doc = new BsonDocument();

    this.startAfter.ifPresent(startAfter -> doc.append(START_AFTER_FIELD, startAfter));
    this.startAtOperationTime.ifPresent(
        startAtOperationTime -> doc.append(START_AT_OPERATION_TIME_FIELD, startAtOperationTime));
    this.fullDocument.ifPresent(
        fullDocument -> doc.append(FULL_DOCUMENT_FIELD, new BsonString(fullDocument)));
    this.showMigrationEvents.ifPresent(
        showMigrationEvents ->
            doc.append(SHOW_MIGRATION_EVENTS_FIELD, new BsonBoolean(showMigrationEvents)));
    this.showExpandedEvents.ifPresent(
        showExpandedEvents ->
            doc.append(SHOW_EXPANDED_EVENTS_FIELD, new BsonBoolean(showExpandedEvents)));
    // matchCollectionUuidForUpdateLookup is not recognized in older MongoDB versions, so we only
    // append it if it is present. MMS will not pass in matchCollectionUuidForUpdateLookup if the
    // MongoDB version is unsupported.
    this.matchCollectionUuidForUpdateLookup.ifPresent(
        matchCollectionUuidForUpdateLookup -> {
          if (matchCollectionUuidForUpdateLookup) {
            doc.append(MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP, new BsonBoolean(true));
          }
        });

    return doc;
  }
}
