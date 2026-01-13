package com.xgen.mongot.index.version;

import com.xgen.mongot.util.Check;
import java.util.Objects;

/**
 * A version number that is associated with a user's index definition and analyzers, it will be
 * incremented any time a user modifies the index definition. A UserIndexVersion is not consistent
 * across a replica set or a shard, and as such, it may only be used within one mongot.
 */
public class UserIndexVersion {

  public static final UserIndexVersion FIRST = new UserIndexVersion(0);

  public final int versionNumber;

  public UserIndexVersion(int versionNumber) {
    Check.argNotNegative(versionNumber, "versionNumber");
    this.versionNumber = versionNumber;
  }

  UserIndexVersion increment() {
    return new UserIndexVersion(this.versionNumber + 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserIndexVersion)) {
      return false;
    }
    UserIndexVersion that = (UserIndexVersion) o;
    return this.versionNumber == that.versionNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.versionNumber);
  }
}
