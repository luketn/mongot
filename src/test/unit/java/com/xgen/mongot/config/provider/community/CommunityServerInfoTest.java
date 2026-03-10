package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;

public class CommunityServerInfoTest {

  @Test
  public void getExternalName_withName_returnsNameConcatenatedWithId() {
    ObjectId id = new ObjectId();
    CommunityServerInfo info = new CommunityServerInfo(id, Optional.of("my-server"));
    assertEquals("my-server." + id.toHexString(), info.getExternalName());
  }

  @Test
  public void getExternalName_withoutName_returnsIdHex() {
    ObjectId id = new ObjectId();
    CommunityServerInfo info = new CommunityServerInfo(id, Optional.empty());
    assertEquals(id.toHexString(), info.getExternalName());
  }
}
