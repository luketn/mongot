package com.xgen.mongot.index.lucene.field;

/** Constants used as values in Lucene indexed fields. Referenced by both index and query logic. */
public class FieldValue {
  public static final String BOOLEAN_TRUE_FIELD_VALUE = "T";
  public static final String BOOLEAN_FALSE_FIELD_VALUE = "F";

  public static final String EMBEDDED_ROOT_FIELD_VALUE = "T";

  public static final String NULL_FIELD_VALUE = "N";

  public static String fromBoolean(boolean value) {
    return value ? FieldValue.BOOLEAN_TRUE_FIELD_VALUE : FieldValue.BOOLEAN_FALSE_FIELD_VALUE;
  }
}
