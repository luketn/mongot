package com.xgen.mongot.server.message;

public enum OpCode {
  REPLY(1),
  MSG_LEGACY(1000),
  UPDATE(2001),
  INSERT(2002),
  RESERVED(2003),
  QUERY(2004),
  GET_MORE(2005),
  DELETE(2006),
  KILL_CURSORS(2007),
  COMMAND(2010),
  COMMAND_REPLY(2011),
  MSG(2013);

  public final int code;

  OpCode(int code) {
    this.code = code;
  }

  /** Produce an OpCode from the supplied code. */
  public static OpCode fromCode(int code) {
    return switch (code) {
      case 1 -> REPLY;
      case 1000 -> MSG_LEGACY;
      case 2001 -> UPDATE;
      case 2002 -> INSERT;
      case 2003 -> RESERVED;
      case 2004 -> QUERY;
      case 2005 -> GET_MORE;
      case 2006 -> DELETE;
      case 2007 -> KILL_CURSORS;
      case 2010 -> COMMAND;
      case 2011 -> COMMAND_REPLY;
      case 2013 -> MSG;
      default -> throw new IllegalArgumentException("unknown op code " + code);
    };
  }
}
