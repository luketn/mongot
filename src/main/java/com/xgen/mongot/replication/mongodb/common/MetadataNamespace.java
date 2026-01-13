package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.xgen.mongot.util.FieldPath;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/*
 * We create a unique internal namespace under indexId field to ensure there is no
 * internal field names collision with the original document and if the aggregation stages
 * that user defines via views modify _id, we still have access to its original value
 */
public class MetadataNamespace {

  public static BsonDocument forRegularQuery(ObjectId indexId) {
    return Aggregates.addFields(new Field<>(indexId + FieldPath.DELIMITER + "_id", "$_id"))
        .toBsonDocument();
  }

  public static BsonDocument forChangeStream(ObjectId indexId) {
    return Aggregates.addFields(
            new Field<>(String.format("fullDocument.%s._id", indexId), "$fullDocument._id"))
        .toBsonDocument();
  }
}
