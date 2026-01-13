package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import java.nio.ByteBuffer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Utility class for dealing with change stream resume tokens. Currently only supports pulling the
 * opTime from the ResumeToken. Extracting any other information requires significantly
 * more-involved decoding.
 */
public class ResumeTokenUtils {

  // Identifier in the binary data that signifies the next entry is a timestamp
  private static final byte TIMESTAMP_IDENTIFIER = (byte) -126;

  /** Extracts and returns the optime from the given resume token. */
  public static BsonTimestamp opTimeFromResumeToken(BsonDocument resumeToken)
      throws DecoderException {
    checkState(resumeToken.isString("_data"), "resumeToken did not have a _data field.");
    byte[] bytes = Hex.decodeHex(resumeToken.getString("_data").getValue());

    checkState(
        bytes[0] == TIMESTAMP_IDENTIFIER,
        "first value of the postBatchResumeToken was not a timestamp.");
    checkState(bytes.length >= 9, "postBatchResumeToken did not contain enough data");

    ByteBuffer wrapped = ByteBuffer.wrap(bytes);
    return new BsonTimestamp(wrapped.getLong(1));
  }
}
