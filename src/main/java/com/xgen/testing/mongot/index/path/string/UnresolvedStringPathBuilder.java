package com.xgen.testing.mongot.index.path.string;

import com.xgen.mongot.index.path.string.UnresolvedStringFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringMultiFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class UnresolvedStringPathBuilder {

  public static UnresolvedStringPath fieldPath(String path) {
    return new UnresolvedStringFieldPath(FieldPath.parse(path));
  }

  /** Creates an UnresolvedStringWildcardPath with the given string. */
  public static UnresolvedStringWildcardPath wildcardPath(String path) {
    Optional<String> message =
        UnresolvedStringPath.containsWildcard(path)
            .flatMap(UnresolvedStringPath::adjacentWildcards);
    if (message.isPresent()) {
      throw new IllegalArgumentException(message.get());
    }
    return new UnresolvedStringWildcardPath(path);
  }

  public static UnresolvedStringMultiFieldPath withMulti(String path, String multi) {
    return new UnresolvedStringMultiFieldPath(FieldPath.parse(path), multi);
  }
}
