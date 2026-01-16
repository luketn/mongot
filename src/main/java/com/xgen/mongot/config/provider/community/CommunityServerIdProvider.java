package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunityServerIdProvider {

  public static final Logger LOG = LoggerFactory.getLogger(CommunityServerIdProvider.class);

  protected static final String SERVER_ID_FILE_NAME = "serverId.txt";

  /**
   * Determines the processes unique server id. The server id is generated and written to disk on
   * initial-startup and re-used for the lifetime of the current server.
   *
   * @param dataPath the server's data path to store the server id too
   * @return the server id for this process
   */
  public static ObjectId getServerId(Path dataPath) {
    Check.checkArg(Files.exists(dataPath), "Data path does not yet exists");
    Check.checkArg(Files.isDirectory(dataPath), "Data path is not a directory");
    Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);
    return getOrCreateServerId(serverIdFilePath);
  }

  private static ObjectId getOrCreateServerId(Path serverIdFilePath) {
    Optional<ObjectId> lastUsedServerId = getLastUsedServerIdFromFile(serverIdFilePath);
    if (lastUsedServerId.isPresent()) {
      LOG.atInfo()
          .addKeyValue("serverId", lastUsedServerId.get())
          .log("Last used server id fetched from file");
      return lastUsedServerId.get();
    }

    ObjectId serverId = generateAndSaveNewServerId(serverIdFilePath);
    LOG.atInfo().addKeyValue("serverId", serverId).log("Generated new server id");
    return serverId;
  }

  private static Optional<ObjectId> getLastUsedServerIdFromFile(Path serverIdFilePath) {
    if (Files.exists(serverIdFilePath)) {
      String hexString =
          Crash.because("error reading server id from file")
              .ifThrows(() -> Files.readString(serverIdFilePath));
      if (StringUtils.isNotBlank(hexString)) {
        return Optional.of(new ObjectId(hexString.trim()));
      }
    }
    return Optional.empty();
  }

  private static ObjectId generateAndSaveNewServerId(Path serverIdFilePath) {
    ObjectId serverId = new ObjectId();
    Crash.because("failed to write server id to file")
        .ifThrows(() -> Files.writeString(serverIdFilePath, serverId.toHexString()));
    return serverId;
  }
}
