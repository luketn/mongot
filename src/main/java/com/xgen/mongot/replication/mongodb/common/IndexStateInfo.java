package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;

public class IndexStateInfo implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<IndexStatus.StatusCode> STATUS =
        Field.builder("status")
            .enumField(IndexStatus.StatusCode.class)
            .asUpperUnderscore()
            .required();
    private static final Field.Required<IndexStatus.Reason> REASON =
        Field.builder("reason").enumField(IndexStatus.Reason.class).asUpperUnderscore().required();
  }

  private final StatusCode status;
  private final IndexStatus.Reason reason;

  private IndexStateInfo(StatusCode status, IndexStatus.Reason reason) {
    this.status = status;
    this.reason = reason;
  }

  public static IndexStateInfo create(StatusCode status, Reason reason) {
    return new IndexStateInfo(status, reason);
  }

  public static IndexStateInfo fromBson(DocumentParser parser) throws BsonParseException {
    return create(parser.getField(Fields.STATUS).unwrap(), parser.getField(Fields.REASON).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.STATUS, this.status)
        .field(Fields.REASON, this.reason)
        .build();
  }

  public IndexStatus.Reason getReason() {
    return this.reason;
  }

  public StatusCode getStatus() {
    return this.status;
  }

  public IndexStatus toIndexStatus() {
    return IndexStatus.fromIndexStateInfo(this.status, this.reason);
  }

  public boolean isCollectionNotFound() {
    return this.getStatus() == IndexStatus.StatusCode.DOES_NOT_EXIST
        && this.getReason() == Reason.COLLECTION_NOT_FOUND;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof IndexStateInfo)) {
      return false;
    }
    IndexStateInfo that = (IndexStateInfo) object;
    return Objects.equals(this.status, that.status) && this.reason == that.reason;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.status, this.reason);
  }
}
