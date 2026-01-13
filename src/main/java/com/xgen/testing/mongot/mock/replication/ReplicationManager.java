package com.xgen.testing.mongot.mock.replication;

import static org.mockito.Mockito.when;

import com.xgen.mongot.util.FutureUtils;
import org.mockito.Mockito;

public class ReplicationManager {

  /** Creates a mock replication manager. */
  public static com.xgen.mongot.replication.ReplicationManager mockReplicationManager() {
    com.xgen.mongot.replication.ReplicationManager replicationManager =
        Mockito.mock(com.xgen.mongot.replication.ReplicationManager.class);
    when(replicationManager.shutdown()).thenReturn(FutureUtils.COMPLETED_FUTURE);
    when(replicationManager.isReplicationSupported()).thenReturn(true);
    return replicationManager;
  }
}
