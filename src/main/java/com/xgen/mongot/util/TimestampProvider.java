package com.xgen.mongot.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/** DateFormats are unsafe for concurrent use, and so, this class is not thread safe. */
public class TimestampProvider {
  private final DateFormat dateFormat;

  private TimestampProvider(DateFormat dateFormat) {
    this.dateFormat = dateFormat;
  }

  /**
   * TimestampProvider formatted in mongod ftdc filename pattern.
   *
   * @return TimestampProvider
   */
  public static TimestampProvider terseTimeStamp() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss'Z'");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return new TimestampProvider(format);
  }

  public String timestamp() {
    return this.dateFormat.format(new Date());
  }
}
