package com.xgen.testing.mongot.index.path.string;

import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.util.FieldPath;

public class StringPathBuilder {

  public static StringFieldPath fieldPath(String path) {
    return new StringFieldPath(FieldPath.parse(path));
  }

  public static StringMultiFieldPath withMulti(String path, String multi) {
    return new StringMultiFieldPath(FieldPath.parse(path), multi);
  }
}
