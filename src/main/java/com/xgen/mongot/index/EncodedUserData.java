package com.xgen.mongot.index;

import java.util.Objects;

/**
 * A wrapper of encoded user data.
 *
 * <p>Currently, the user data can only be encoded as a String.
 */
public class EncodedUserData {
  public static final EncodedUserData EMPTY = EncodedUserData.fromString("{}");
  private final String userDataString;

  private EncodedUserData(String userDataString) {
    this.userDataString = userDataString;
  }

  public static EncodedUserData fromString(String userDataString) {
    return new EncodedUserData(userDataString);
  }

  public String asString() {
    return this.userDataString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncodedUserData that = (EncodedUserData) o;
    return Objects.equals(this.userDataString, that.userDataString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.userDataString);
  }
}
