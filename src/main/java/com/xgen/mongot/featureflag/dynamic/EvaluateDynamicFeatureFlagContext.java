package com.xgen.mongot.featureflag.dynamic;

import com.xgen.mongot.index.query.Query;
import java.nio.ByteBuffer;
import org.bson.types.ObjectId;

record EvaluateDynamicFeatureFlagContext(ObjectId entityId, byte[] entityByteArray) {

  private static final ObjectId unusedObjId = new ObjectId();

  public EvaluateDynamicFeatureFlagContext(ObjectId entityId) {
    this(entityId, entityId.toByteArray());
  }

  public EvaluateDynamicFeatureFlagContext(Query query) {
    this(
        unusedObjId, // unused
        ByteBuffer.allocate(4).putInt(System.identityHashCode(query)).array());
  }
}
