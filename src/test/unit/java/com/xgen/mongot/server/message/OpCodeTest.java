package com.xgen.mongot.server.message;

import static org.junit.Assert.assertSame;

import org.junit.Test;

public class OpCodeTest {

  @Test
  public void fromCodeIsBijective() {
    for (OpCode original : OpCode.values()) {
      OpCode result = OpCode.fromCode(original.code);
      assertSame(original, result);
    }
  }
}
