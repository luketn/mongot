package com.xgen.mongot.util.mongodb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Databases {
  public static final String ADMIN = "admin";
  public static final String LOCAL = "local";

  public static final Set<Character> PROHIBITED_CHARACTERS =
      new HashSet<>(Arrays.asList('\0', '/', '\\', ' ', '"', '.', '$'));
}
