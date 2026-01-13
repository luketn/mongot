package com.xgen.mongot.server.message;

public record MessageHeader(int messageLength, int requestId, int responseTo, OpCode opCode) {

  public static final int SIZE_IN_BYTES = 16;
}
