package com.xgen.mongot.util.bson.parser;

/** TypeDescription contains descriptions of different data types intended to be read by a human. */
public class TypeDescription {
  public static final String ARRAY = "array";
  public static final String BINARY = "binary";
  public static final String BOOLEAN = "boolean";
  public static final String DATE_TIME = "date time";
  public static final String DOCUMENT = "document";
  public static final String FLOAT = "32 bit floating point number";
  public static final String INT_32 = "32 bit integer";
  public static final String INT_64 = "64 bit integer";
  public static final String INTEGER = "integer";
  public static final String NUMBER = "number";
  public static final String NUMBER_NOT_DECIMAL = "int32, int64, or double";
  public static final String OBJECT_ID = "ObjectId";
  public static final String STRING = "string";
  public static final String TIMESTAMP = "timestamp";
  public static final String UUID = "UUID";

  private TypeDescription() {}
}
