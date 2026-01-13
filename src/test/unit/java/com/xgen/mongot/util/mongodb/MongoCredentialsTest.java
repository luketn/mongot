package com.xgen.mongot.util.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.xgen.mongot.util.mongodb.MongoCredentialsTest.TestBadFileCases;
import com.xgen.mongot.util.mongodb.MongoCredentialsTest.TestFailureCases;
import com.xgen.mongot.util.mongodb.MongoCredentialsTest.TestSuccessCases;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({TestSuccessCases.class, TestFailureCases.class, TestBadFileCases.class})
public class MongoCredentialsTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper(YAMLFactory.builder().disable(Feature.WRITE_DOC_START_MARKER).build());

  @RunWith(Parameterized.class)
  public static class TestSuccessCases {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameter(0)
    public String fileContents;

    @Parameterized.Parameter(1)
    public String expectedKey;

    @Parameters(name = "MongoCredentialsTest.TestSuccessCases {index}:{0}")
    public static List<String[]> args() {
      return List.of(
          new String[] {"encryptedKeyFile", "encryptedKeyFile"},
          new String[] {
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/=",
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/="
          },
          new String[] {
            """
            white space
            strip\ttest\u000a\u000b\u000c\u000d
            hellooo""",
            "whitespacestriptesthellooo"
          });
    }

    @Test
    public void expectSuccessSingle() throws IOException {
      var tempFile = this.tempFolder.newFile().toPath();
      Files.writeString(tempFile, this.fileContents);
      assertThat(MongoCredentials.readKeyFile(tempFile)).isEqualTo(this.expectedKey);
    }

    @Test
    public void expectSuccessList() throws IOException {
      var validTwice = this.tempFolder.newFile();
      MAPPER.writeValue(validTwice, List.of(this.fileContents, this.fileContents));
      assertThat(MongoCredentials.readKeyFile(validTwice.toPath())).isEqualTo(this.expectedKey);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestFailureCases {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameter public String fileContents;

    @Parameters(name = "MongoCredentialsTest.TestFailureCases {index}")
    public static String[] args() {
      return new String[] {
        "aaaaa", // too short
        "y\n\n\n\t\t\t".repeat(1000), // key not too long, but file is too long.
        "x".repeat(1025), // read, but too long
        "aaaaaa\u0000", // invalid null char
        "aaa-_aaa" // invalid base 64 chars
      };
    }

    @Test
    public void expectFailureSingle() throws IOException {
      var fileWithSingleKey = this.tempFolder.newFile().toPath();
      Files.writeString(fileWithSingleKey, this.fileContents);
      assertThrows(
          "test case: " + this.fileContents,
          Exception.class,
          () -> MongoCredentials.readKeyFile(fileWithSingleKey));
    }

    @Test
    public void expectFailureFirst() throws IOException {
      var firstKeyInvalid = this.tempFolder.newFile();
      MAPPER.writeValue(firstKeyInvalid, List.of(this.fileContents, "aaaaaa"));
      assertThrows(
          "test case: " + this.fileContents,
          IllegalArgumentException.class,
          () -> MongoCredentials.readKeyFile(firstKeyInvalid.toPath()));
    }

    @Test
    public void expectFailureSecond() throws IOException {
      var secondKeyInvalid = this.tempFolder.newFile();
      MAPPER.writeValue(secondKeyInvalid, List.of("aaaaaa", this.fileContents));
      assertThrows(
          "test case: " + this.fileContents,
          IllegalArgumentException.class,
          () -> MongoCredentials.readKeyFile(secondKeyInvalid.toPath()));
    }
  }

  public static class TestBadFileCases {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void fileDoesNotExist() {
      var fileNotExist = this.tempFolder.getRoot().toPath().resolve("does-not-exist");
      assertThrows(
          IllegalArgumentException.class, () -> MongoCredentials.readKeyFile(fileNotExist));
    }

    @Test
    public void fileContainsObject() throws IOException {
      var notArrayOrString = this.tempFolder.newFile();
      MAPPER.writeValue(notArrayOrString, Map.of("key1", "validkeyhere"));
      assertThrows(
          IllegalArgumentException.class,
          () -> MongoCredentials.readKeyFile(notArrayOrString.toPath()));
    }
  }
}
