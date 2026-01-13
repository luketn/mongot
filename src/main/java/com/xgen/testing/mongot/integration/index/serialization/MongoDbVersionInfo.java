package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.BsonValue;

public class MongoDbVersionInfo implements Encodable {
  static class Values {
    static final Value.Required<String> VERSION = Value.builder().stringValue().required();
  }

  private final String version;
  private final int major;
  private final int minor;

  private final int patch;

  /**
   * Tag integration tests with "disabled" if you would like to skip testing for all mongodb
   * versions. An example could be skipping tests for a feature on all sharded deployments but still
   * testing the feature on all non-sharded deployments.
   */
  private static final String DISABLED = "disabled";

  /**
   * VERSION_PATTERN will be used for extracting mongodb version from a string for integration
   * tests. This is used for skipping some features that are not supported with older mongodb
   * versions.
   *
   * <p>Major and minor is required from the input string(e.g "6.2") and patch will be set as 0 if
   * it is missing."prerelease" and "buildmetadata" is also optional as the versionInfo from latest
   * master could contain these fields (e.g "8.3.0-alpha0-617-g432dbba")
   */
  public static Pattern VERSION_PATTERN =
      Pattern.compile(
          "^(?<major>0|[1-9]\\d*)\\.(?<minor>0|[1-9]\\d*)(?:\\.(?<patch>0|[1-9]\\d*))?"
              + "(?:-(?<prerelease>(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)"
              + "(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
              + "(?:\\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

  public MongoDbVersionInfo(String version) {
    this.version = version;
    if (version.equals(DISABLED)) {
      this.major = 0;
      this.minor = 0;
      this.patch = 0;
      return;
    }
    Matcher matcher = VERSION_PATTERN.matcher(version);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("invalid version: " + version);
    }

    try {
      this.major = Integer.parseInt(matcher.group("major"));
      this.minor = Integer.parseInt(matcher.group("minor"));
      this.patch = Optional.ofNullable(matcher.group("patch")).map(Integer::parseInt).orElse(0);

    } catch (NumberFormatException | NullPointerException e) {
      throw new IllegalArgumentException("invalid version: " + version);
    }
  }

  static MongoDbVersionInfo fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    return new MongoDbVersionInfo(Values.VERSION.getParser().parse(context, bsonValue));
  }

  public boolean isMongoDbVersionSupported(String dbVersion) {
    if (this.version.equals(DISABLED)) {
      return false;
    }

    MongoDbVersionInfo dbVersionInfo = new MongoDbVersionInfo(dbVersion);

    if (this.major != dbVersionInfo.major) {
      return dbVersionInfo.major - this.major > 0;
    }
    if (this.minor != dbVersionInfo.minor) {
      return dbVersionInfo.minor - this.minor > 0;
    }
    if (this.patch != dbVersionInfo.patch) {
      return dbVersionInfo.patch - this.patch > 0;
    }
    return true;
  }

  /**
   * isNaturalOrderScanSupported() will be used for skipping natural order scan tests that are not
   * supported with older mongodb versions.
   */
  public static boolean isNaturalOrderScanSupported(String dbVersion) {
    MongoDbVersionInfo version = new MongoDbVersionInfo(dbVersion);

    if (version.major < 8) {
      return false; // All 7.x.x are invalid
    }
    if (version.major > 8) {
      return true; // 9.x.x and above are valid
    }
    if (version.minor >= 3) {
      return true; // 8.3.x and above are valid
    }
    if (version.minor == 0) {
      return version.patch >= 14; // only 8.0.14 and above are valid
    }
    if (version.minor == 1) {
      return false; // All 8.1.x are invalid
    }
    if (version.minor == 2) {
      return version.patch >= 1; // Only 8.2.1 and above are invalid
    }
    return false; // fallback
  }

  @Override
  public BsonValue toBson() {
    return Values.VERSION.getEncoder().encode(this.version);
  }

  @Override
  public String toString() {
    return String.format("[%s]", this.version);
  }
}
