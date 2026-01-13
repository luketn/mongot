package com.xgen.mongot.util.mongodb;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.operation.ReadOperation;
import org.junit.Test;

public class BatchMongoClientTest {

  @Test
  public void testDefaultConnectionStringValues() {
    var connectionString = new ConnectionString("mongodb://localhost:37017/?authSource=admin");
    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      verify(readOperation)
          .execute(
              argThat(
                  binding ->
                      checkReadPreferenceAndConcern(
                          binding, ReadPreference.primary(), ReadConcern.DEFAULT)));
    }
  }

  @Test
  public void testReadPreferenceSettingProvidedByConnectionString() {
    var connectionString =
        new ConnectionString(
            "mongodb://localhost:37017/?authSource=admin"
                + "&readConcernLevel=majority"
                + "&readPreference=secondary");

    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

    try (var mongoClient = new BatchMongoClient(settings)) {
      var readOperation = mock(ReadOperation.class);
      var session = mock(ClientSession.class);
      when(session.getOptions()).thenReturn(ClientSessionOptions.builder().build());

      mongoClient.openCursor(readOperation, session);

      verify(readOperation)
          .execute(
              argThat(
                  binding ->
                      checkReadPreferenceAndConcern(
                          binding, ReadPreference.secondary(), ReadConcern.MAJORITY)));
    }
  }

  private boolean checkReadPreferenceAndConcern(
      ReadBinding binding, ReadPreference readPreference, ReadConcern readConcern) {

    return binding.getReadPreference().equals(readPreference)
        && binding.getSessionContext().getReadConcern().equals(readConcern);
  }
}
