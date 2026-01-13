package com.xgen.mongot.util.mongodb;

import com.mongodb.MongoException;
import java.util.List;

/**
 * Class to define and compare <code>MongoDbVersions</code>. It is important to note that this class
 * does not track any pre-release version information associated with the specific major/minor/patch
 * version specified.
 */
public record MongoDbVersion(int major, int minor, int patch)
    implements Comparable<MongoDbVersion> {

  /**
   * Utilizes the input from the <code>buildInfo.versionArray</code> field to create a <code>
   * MongoDbVersion</code> while ignoring the pre-release version number specified in the
   * versionArray.
   */
  public static MongoDbVersion fromVersionArray(List<Integer> versionArray)
      throws CheckedMongoException {
    if (versionArray.size() < 3) {
      throw new CheckedMongoException(
          new MongoException("size of buildInfo.versionArray must be at least 3"));
    }
    return new MongoDbVersion(versionArray.get(0), versionArray.get(1), versionArray.get(2));
  }

  @Override
  public int compareTo(MongoDbVersion other) {
    int majorCompare = Integer.compare(this.major, other.major);
    if (majorCompare != 0) {
      return majorCompare;
    }

    int minorCompare = Integer.compare(this.minor, other.minor);
    if (minorCompare != 0) {
      return minorCompare;
    }

    return Integer.compare(this.patch, other.patch);
  }

  public String toVersionString() {
    return String.format("%d.%d.%d", this.major, this.minor, this.patch);
  }
}
