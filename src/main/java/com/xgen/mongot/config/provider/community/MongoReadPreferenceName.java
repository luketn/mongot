package com.xgen.mongot.config.provider.community;

import com.mongodb.ReadPreference;

public enum MongoReadPreferenceName {
  PRIMARY,
  PRIMARY_PREFERRED,
  SECONDARY,
  SECONDARY_PREFERRED,
  NEAREST;

  public ReadPreference asReadPreference() {
    return switch (this) {
      case PRIMARY -> ReadPreference.primary();
      case PRIMARY_PREFERRED -> ReadPreference.primaryPreferred();
      case SECONDARY -> ReadPreference.secondary();
      case SECONDARY_PREFERRED -> ReadPreference.secondaryPreferred();
      case NEAREST -> ReadPreference.nearest();
    };
  }
}
