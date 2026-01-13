package com.xgen.mongot.server.message;

import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;

public record QueryMessage(
    MessageHeader messageHeader,
    int flags,
    String namespace,
    int skip,
    int numReturn,
    RawBsonDocument query,
    Optional<RawBsonDocument> project)
    implements InboundMessage {

  @Override
  public MessageHeader getHeader() {
    return this.messageHeader;
  }

  @Override
  public OutboundMessage getOutboundMessage(BsonDocument body) {
    return new ReplyMessage(this.getHeader(), 0, 0L, 0, body);
  }

  @Override
  public String toString() {
    return new Document()
        .append("header", this.messageHeader)
        .append("flags", this.flags)
        .append("namespace", this.namespace)
        .append("skip", this.skip)
        .append("nReturn", this.numReturn)
        .append("query", this.query)
        .append("project", this.project)
        .toString();
  }

  public static QueryMessage fromBytes(MessageHeader messageHeader, ByteBuf body) {

    int flags = body.readIntLE();
    String namespace = MessageUtils.readCString(body);
    int skip = body.readIntLE();
    int numReturn = body.readIntLE();
    RawBsonDocument query = MessageUtils.rawBsonDocumentFromBytes(body);

    Optional<RawBsonDocument> project;
    if (body.readableBytes() == 0) {
      project = Optional.empty();
    } else {
      project = Optional.of(MessageUtils.rawBsonDocumentFromBytes(body));
    }

    return new QueryMessage(messageHeader, flags, namespace, skip, numReturn, query, project);
  }
}
