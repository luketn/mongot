package com.xgen.mongot.util.mongodb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReplicaSetSignatureTest {

  @Test
  public void testFromBasePrefix() {
    var rsId = "atlas-xyz";
    var timeSuffix = "20250601T120000_123Z";
    var basePrefix =
        String.format(
            "snapshots/647343c92726408e854130e5/647347c92726405e854130e6/%s-%s", rsId, timeSuffix);
    var signature = ReplicaSetSignature.fromSnapshotterBasePrefix(basePrefix);
    assertEquals(new ReplicaSetSignature(rsId, timeSuffix), signature);
  }
}
