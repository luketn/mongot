package com.xgen.mongot.server.command;

import com.xgen.mongot.server.message.InboundMessage;
import com.xgen.mongot.server.message.MessageHeader;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.MessageSection;
import com.xgen.mongot.server.message.MessageSectionBody;
import com.xgen.mongot.server.message.MessageSectionDocumentSequence;
import com.xgen.mongot.server.message.OpCode;
import com.xgen.mongot.server.message.QueryMessage;
import java.util.ArrayList;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class WireCommandParserTest {

  @Test
  public void parseOpMessage() {
    ArrayList<MessageSection> sections = new ArrayList<>();
    MessageSectionBody section =
        new MessageSectionBody(new BsonDocument().append("hello", BsonBoolean.TRUE));
    sections.add(section);
    MessageMessage opMsg =
        new MessageMessage(new MessageHeader(0, 233, 0, OpCode.MSG), 0, sections);

    ParsedCommand parseOpMsgResult = WireCommandParser.parse(opMsg);
    Assert.assertEquals("hello", parseOpMsgResult.name());
    Assert.assertEquals(parseOpMsgResult.body(), section.body);

    ParsedCommand parseInboundMessageResult = WireCommandParser.parse((InboundMessage) opMsg);
    Assert.assertEquals("hello", parseInboundMessageResult.name());
    Assert.assertEquals(parseInboundMessageResult.body(), section.body);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseOpMessageEmptySections() {
    ArrayList<MessageSection> sections = new ArrayList<>();
    MessageMessage opMsg =
        new MessageMessage(new MessageHeader(0, 233, 0, OpCode.MSG), 0, sections);
    WireCommandParser.parse(opMsg);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseOpMessageWithDocumentSequence() {
    ArrayList<MessageSection> sections = new ArrayList<>();
    sections.add(new MessageSectionDocumentSequence("id", new ArrayList<BsonDocument>()));
    MessageMessage opMsg =
        new MessageMessage(new MessageHeader(0, 233, 0, OpCode.MSG), 0, sections);
    WireCommandParser.parse(opMsg);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseOpMessageNoFirstKey() {
    ArrayList<MessageSection> sections = new ArrayList<>();
    MessageSectionBody section = new MessageSectionBody(new BsonDocument());
    sections.add(section);
    MessageMessage opMsg =
        new MessageMessage(new MessageHeader(0, 233, 0, OpCode.MSG), 0, sections);
    WireCommandParser.parse(opMsg);
  }

  @Test
  public void parseQueryMessage() {
    MessageSectionBody section =
        new MessageSectionBody(new BsonDocument().append("hello", BsonBoolean.TRUE));
    QueryMessage queryMsg =
        new QueryMessage(
            new MessageHeader(0, 233, 0, OpCode.MSG),
            0,
            "admin.$cmd",
            0,
            0,
            section.body,
            Optional.empty());

    ParsedCommand parseQueryMsgResult = WireCommandParser.parse(queryMsg);
    Assert.assertEquals("hello", parseQueryMsgResult.name());
    Assert.assertEquals(parseQueryMsgResult.body(), section.body);

    ParsedCommand parseInboundMessageResult = WireCommandParser.parse((InboundMessage) queryMsg);
    Assert.assertEquals("hello", parseInboundMessageResult.name());
    Assert.assertEquals(parseInboundMessageResult.body(), section.body);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseQueryMessageInvalidNamespace() {
    MessageSectionBody section =
        new MessageSectionBody(new BsonDocument().append("hello", BsonBoolean.TRUE));
    QueryMessage queryMsg =
        new QueryMessage(
            new MessageHeader(0, 233, 0, OpCode.MSG),
            0,
            "invalid",
            0,
            0,
            section.body,
            Optional.empty());
    WireCommandParser.parse(queryMsg);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseQueryMessageNoFirstKey() {
    MessageSectionBody section = new MessageSectionBody(new BsonDocument());
    QueryMessage queryMsg =
        new QueryMessage(
            new MessageHeader(0, 233, 0, OpCode.MSG),
            0,
            "admin.$cmd",
            0,
            0,
            section.body,
            Optional.empty());
    WireCommandParser.parse(queryMsg);
  }
}
