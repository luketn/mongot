package com.xgen.mongot.index.definition;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.collections4.trie.UnmodifiableTrie;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class StoredSourceDefinition implements Encodable {

  private static final FieldPath ID_PATH = FieldPath.fromParts("_id");
  private static final String EXPECTED_TYPES = "boolean or document";

  static class Fields {
    static final Field.Optional<List<String>> INCLUDE =
        Field.builder("include")
            .stringField()
            .mustNotBeBlank()
            .asList()
            .mustNotBeEmpty()
            .optional()
            .noDefault();

    static final Field.Optional<List<String>> EXCLUDE =
        Field.builder("exclude")
            .stringField()
            .mustNotBeBlank()
            .asList()
            .mustNotBeEmpty()
            .optional()
            .noDefault();
  }

  public enum Mode {
    INCLUSION,
    EXCLUSION;

    private Field.Optional<List<String>> correspondingField() {
      return this == INCLUSION ? Fields.INCLUDE : Fields.EXCLUDE;
    }
  }

  private final Mode mode;
  private final UnmodifiableTrie<String, Node> paths;

  public static StoredSourceDefinition createIncludeAll() {
    return create(Mode.EXCLUSION, List.of());
  }

  public static StoredSourceDefinition createExcludeAll() {
    return create(Mode.INCLUSION, List.of());
  }

  public static StoredSourceDefinition create(Mode mode, List<String> dottedPaths) {
    return new StoredSourceDefinition(mode, createFromDottedPaths(dottedPaths));
  }

  private StoredSourceDefinition(Mode mode, UnmodifiableTrie<String, Node> paths) {
    this.mode = mode;
    this.paths = paths;
  }

  public Mode getMode() {
    return this.mode;
  }

  /**
   * Returns true if the given path ends with a stored field. E.g. if `a.b.c` is configured as
   * included, then paths `a.b.c` and `a.b.c.x` are "stored"; if `a.b.c` is configured as excluded,
   * than everything except the sub-tree under `a.b.c` is stored.
   */
  public boolean isStored(FieldPath path) {
    if (isInclusion()) {
      return this.asInclusion().isIncluded(path);
    }
    return !this.asExclusion().isExcluded(path);
  }

  /**
   * Returns true if the given path ends with a stored field or leads to a stored field. E.g. if
   * `a.b.c` is included, then paths `a`, `a.b`, `a.b.c` and `a.b.c.x` are the "paths to stored"; if
   * `a.b.c` is excluded, than any node except those under `a.b.c` are on the "paths to stored".
   */
  public boolean isPathToStored(FieldPath path) {
    if (isInclusion()) {
      return this.asInclusion().isPathToIncluded(path);
    }
    return !this.asExclusion().isExcluded(path);
  }

  public boolean isAllIncluded() {
    return this.paths.isEmpty() && this.mode == Mode.EXCLUSION;
  }

  public boolean isAllExcluded() {
    return this.paths.isEmpty() && this.mode == Mode.INCLUSION;
  }

  public boolean isInclusion() {
    return isAllIncluded() || this.mode == Mode.INCLUSION;
  }

  public boolean isExclusion() {
    return isAllExcluded() || this.mode == Mode.EXCLUSION;
  }

  public Inclusion asInclusion() {
    checkArg(isInclusion(), "Invalid mode");
    return new Inclusion();
  }

  public Exclusion asExclusion() {
    checkArg(isExclusion(), "Invalid mode");
    return new Exclusion();
  }

  public static StoredSourceDefinition fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    return switch (bsonValue.getBsonType()) {
      case BOOLEAN -> fromBsonBoolean(bsonValue.asBoolean());
      case DOCUMENT -> fromBsonDocument(context, bsonValue.asDocument());
      default -> context.handleUnexpectedType(EXPECTED_TYPES, bsonValue.getBsonType());
    };
  }

  public static StoredSourceDefinition defaultValue() {
    return createExcludeAll();
  }

  /**
   * Returns stored source paths as dotted strings. The output is guaranteed to have no path
   * collisions.
   */
  public List<String> getDottedPaths() {
    return this.paths.entrySet().stream()
        .filter(e -> e.getValue().isLeaf)
        .map(Map.Entry::getKey)
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public BsonValue toBson() {

    if (isAllIncluded()) {
      return new BsonBoolean(true);
    }

    if (isAllExcluded()) {
      return new BsonBoolean(false);
    }

    return BsonDocumentBuilder.builder()
        .field(this.mode.correspondingField(), Optional.of(getDottedPaths()))
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoredSourceDefinition)) {
      return false;
    }
    StoredSourceDefinition that = (StoredSourceDefinition) o;
    return this.mode == that.mode && this.paths.equals(that.paths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.mode, this.paths);
  }

  private static StoredSourceDefinition fromBsonBoolean(BsonBoolean bool) {
    return bool.getValue() ? createIncludeAll() : createExcludeAll();
  }

  private static StoredSourceDefinition fromBsonDocument(
      BsonParseContext context, BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.withContext(context, document).build()) {
      var include = parser.getField(Fields.INCLUDE);
      var exclude = parser.getField(Fields.EXCLUDE);
      var dottedPaths = parser.getGroup().exactlyOneOf(include, exclude);
      var mode = include.unwrap().isPresent() ? Mode.INCLUSION : Mode.EXCLUSION;
      return create(mode, dottedPaths);

    } catch (IllegalArgumentException e) {
      return context.handleSemanticError(e.getMessage());
    }
  }

  private static UnmodifiableTrie<String, Node> createFromDottedPaths(List<String> dottedPaths) {

    var paths = new PatriciaTrie<Node>();

    for (var dottedPath : dottedPaths) {
      var path = FieldPath.parse(dottedPath);
      addPath(paths, dottedPath, Node.leaf());
      path.ancestorPaths()
          .map(FieldPath::toString)
          .forEach(ancestor -> addPath(paths, ancestor, Node.intermediate()));
    }

    return new UnmodifiableTrie<>(paths);
  }

  private static void addPath(Trie<String, Node> paths, String path, Node node) {
    Optional.ofNullable(paths.put(path, node))
        .ifPresent(
            existing -> checkArg(existing.isLeaf == node.isLeaf, "path collision is not allowed"));
  }

  private boolean isIncludedImplicitly(FieldPath path) {
    return path.equals(ID_PATH);
  }

  public static class Node {

    public final boolean isLeaf;

    private Node(boolean isLeaf) {
      this.isLeaf = isLeaf;
    }

    public static Node leaf() {
      return new Node(true);
    }

    public static Node intermediate() {
      return new Node(false);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Node) {
        return this.isLeaf == ((Node) o).isLeaf;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.isLeaf);
    }
  }

  public class Inclusion {

    private Inclusion() {}

    /**
     * Returns true if the given path ends with a stored field. E.g. if `a.b.c` is configured as
     * stored, then paths `a.b.c` and `a.b.c.x` are "stored". Note that some fields are implicitly
     * included by default.
     */
    public boolean isIncluded(FieldPath path) {

      if (isAllExcluded()) {
        return false;
      }

      if (isAllIncluded()) {
        return true;
      }

      var isLeaf =
          Optional.ofNullable(StoredSourceDefinition.this.paths.get(path.toString()))
              .map(p -> p.isLeaf)
              .orElse(false);

      var isUnderLeaf =
          path.ancestorPaths()
              .map(p -> StoredSourceDefinition.this.paths.get(p.toString()))
              .anyMatch(p -> Objects.nonNull(p) && p.isLeaf);

      return isLeaf || isUnderLeaf || isIncludedImplicitly(path);
    }

    /**
     * Returns true if the given path ends with a stored field or leads to a stored field. E.g. if
     * `a.b.c` is included, then paths `a`, `a.b`, `a.b.c` and `a.b.c.x` are the "paths to the
     * included".
     */
    boolean isPathToIncluded(FieldPath path) {
      return StoredSourceDefinition.this.paths.containsKey(path.toString()) || isIncluded(path);
    }
  }

  public class Exclusion {

    private Exclusion() {}

    /**
     * Returns true if the given path ends with an excluded field. E.g. if `a.b.c` is excluded, then
     * paths `a.b.c` and `a.b.c.x` are excluded.
     */
    public boolean isExcluded(FieldPath path) {

      if (isAllIncluded()) {
        return false;
      }

      if (isAllExcluded()) {
        return true;
      }

      var isLeaf =
          Optional.ofNullable(StoredSourceDefinition.this.paths.get(path.toString()))
              .map(p -> p.isLeaf)
              .orElse(false);

      var isUnderLeaf =
          path.ancestorPaths()
              .map(p -> StoredSourceDefinition.this.paths.get(p.toString()))
              .anyMatch(p -> Objects.nonNull(p) && p.isLeaf);

      return isLeaf || isUnderLeaf;
    }
  }
}
