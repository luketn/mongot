package com.xgen.mongot.config.provider.community;

import static com.xgen.mongot.config.provider.community.CommunityServerIdProvider.SERVER_ID_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommunityServerIdProviderTest {

  @Test
  public void testGetServerId_DataPathDoesNotExist_Throws() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      assertThrows(
          IllegalArgumentException.class,
          () ->
              CommunityServerIdProvider.getServerId(
                  folder.getRoot().toPath().resolve("does-not-exist")));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_DataPathIsFile_Throws() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path storagePathIsFile = folder.newFile("temp-file").toPath();
      assertThrows(
          IllegalArgumentException.class,
          () -> CommunityServerIdProvider.getServerId(storagePathIsFile));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_IoExceptionReadingFromFile_Throws() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      File serverIdNameFile = folder.newFile(SERVER_ID_FILE_NAME);
      serverIdNameFile.setReadable(false);

      assertThrows(
          SecurityException.class, // Crash triggers security exception in unit tests
          () -> CommunityServerIdProvider.getServerId(folder.getRoot().toPath()));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_IoExceptionWritingToFile_Throws() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      Files.createFile(serverIdFilePath);
      serverIdFilePath.toFile().setWritable(false);

      assertThrows(
          SecurityException.class, // Crash triggers security exception in unit tests
          () -> CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_ServerIdFileExists_ReturnServerId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      ObjectId serverId = new ObjectId();
      Files.writeString(dataPath.resolve(SERVER_ID_FILE_NAME), serverId.toHexString());

      assertEquals(serverId, CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_ServerIdFileDoesNotExists_GeneratesNewServerId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();

      ObjectId serverId = CommunityServerIdProvider.getServerId(dataPath);
      assertNotNull(serverId);

      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);
      assertTrue(Files.exists(serverIdFilePath));
      assertEquals(serverId.toHexString(), Files.readString(serverIdFilePath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_ServerIdFileExistsButWhitespace_GeneratesNewServerId()
      throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      Files.writeString(serverIdFilePath, "   \n\t  ");

      ObjectId serverId = CommunityServerIdProvider.getServerId(dataPath);
      assertNotNull(serverId);

      // Verify the new server ID was written to the file
      String fileContent = Files.readString(serverIdFilePath);
      assertEquals(serverId.toHexString(), fileContent);
      assertFalse(fileContent.isEmpty());
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_CalledMultipleTimes_ReturnsSameServerId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();

      ObjectId firstServerId = CommunityServerIdProvider.getServerId(dataPath);
      assertNotNull(firstServerId);

      ObjectId secondServerId = CommunityServerIdProvider.getServerId(dataPath);
      assertEquals(firstServerId, secondServerId);

      ObjectId thirdServerId = CommunityServerIdProvider.getServerId(dataPath);
      assertEquals(firstServerId, thirdServerId);
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_ValidObjectIdInFile_ReturnsCorrectObjectId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      ObjectId expectedServerId = new ObjectId("507f1f77bcf86cd799439011");
      Files.writeString(serverIdFilePath, expectedServerId.toHexString());

      assertEquals(expectedServerId, CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_InvalidObjectIdInFile_Throws() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);
      Files.writeString(serverIdFilePath, "invalid-object-id");

      assertThrows(
          IllegalArgumentException.class, () -> CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_FileWithTrailingWhitespace_ReturnsCorrectObjectId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      ObjectId expectedServerId = new ObjectId();
      Files.writeString(serverIdFilePath, expectedServerId.toHexString() + "  \n");

      assertEquals(expectedServerId, CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_FileWithLeadingWhitespace_ReturnsCorrectObjectId() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      ObjectId expectedServerId = new ObjectId();
      Files.writeString(serverIdFilePath, "  \n" + expectedServerId.toHexString());

      assertEquals(expectedServerId, CommunityServerIdProvider.getServerId(dataPath));
    } finally {
      folder.delete();
    }
  }

  @Test
  public void testGetServerId_FilePersistedCorrectly() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    try {
      folder.create();
      Path dataPath = folder.getRoot().toPath();
      Path serverIdFilePath = dataPath.resolve(SERVER_ID_FILE_NAME);

      // Generate a new server ID
      ObjectId serverId = CommunityServerIdProvider.getServerId(dataPath);

      // Verify the file exists and contains the correct content
      assertTrue(Files.exists(serverIdFilePath));
      assertTrue(Files.isRegularFile(serverIdFilePath));
      assertTrue(serverIdFilePath.toFile().canRead());
      assertEquals(serverId.toHexString(), Files.readString(serverIdFilePath));
    } finally {
      folder.delete();
    }
  }
}
