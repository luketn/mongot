package com.xgen.mongot.server.message;

import org.bson.BsonDocument;

public interface InboundMessage {

  /* Returns the message header */
  MessageHeader getHeader();

  /* Creates a corresponding OutboundMessage for this InboundMessage */
  OutboundMessage getOutboundMessage(BsonDocument body);
}
