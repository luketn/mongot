package com.xgen.mongot.index.lucene.query.pushdown.project;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.io.ByteBufferBsonInput;

/**
 * A ProjectStage that preserves all fields in an input doc except those explicitly excluded.
 *
 * <p>The server's behavior for repeated keys is explicitly documented as undefined. In practice,
 * excluding a repeated key only excludes the first occurrence. This behavior is replicated in our
 * initial implementation, but we should have the freedom to change this for performance reasons in
 * the future.
 */
class ExclusiveBlobProjection implements ProjectionTransform<RawBsonDocument, RawBsonDocument> {

  private final PathTrie<Boolean> trie;

  ExclusiveBlobProjection(ProjectSpec spec) {
    this.trie = new PathTrie<>();
    for (FieldPath path : spec.pathsToExclude) {
      this.trie.put(path, true);
    }
  }

  @Override
  public RawBsonDocument project(RawBsonDocument document) {
    ByteBuf src = document.getByteBuffer();
    BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(src));
    ByteBuffer out = ByteBuffer.allocate(src.remaining()).order(ByteOrder.LITTLE_ENDIAN);
    byte[] raw = src.array();

    copyWithExclusions(reader, raw, out, this.trie);
    return new RawBsonDocument(out.array(), 0, out.position());
  }

  /**
   * This method is used when we have an array as an ancestor of an excluded path. We can't copy the
   * entire array, and instead need to recursively unnest arrays and do a partial copy of any child
   * documents.
   */
  private static void copyPartialArray(
      BsonBinaryReader reader, byte[] raw, ByteBuffer out, PathTrie<Boolean> trie) {
    int sizeOffset = out.position();
    @Var int spanStart = reader.getBsonInput().getPosition();
    reader.readStartArray();

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.getCurrentBsonType()) {
        case ARRAY -> {
          // Unlike $match, we expand any number of nested arrays
          out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
          copyPartialArray(reader, raw, out, trie);
          spanStart = reader.getBsonInput().getPosition();
        }
        case DOCUMENT -> {
          // $project paths don't include array indices, so we just forward the call to the document
          out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
          copyWithExclusions(reader, raw, out, trie);
          spanStart = reader.getBsonInput().getPosition();
        }
        default ->
            // Leaf values can't contain paths, so we can just skip this value
            reader.skipValue();
      }
    }
    reader.readEndArray();
    out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
    out.putInt(sizeOffset, out.position() - sizeOffset);
  }

  /**
   * Copies a (possibly nested) document to a buffer while excluding fields but otherwise maintains
   * the order of the original document.
   *
   * <p>This implementation attempts to maximize the length of contiguous array copies without
   * object allocations. To do this, suppose we have document with keys {a, b, c, d, e} and we only
   * want to exclude 'c'. First, we copy raw bytes from the header until 'c', then copy bytes from
   * 'd' until the end of the document. Finally, we update the document header with the correct
   * length.
   *
   * @param reader - a BsonReader positioned at the start of a new Document
   * @param raw - the underlying content of `reader`
   * @param out - the bytebuffer to copy content to
   * @param trie - a set of paths to exclude relative to the current document
   */
  private static void copyWithExclusions(
      BsonBinaryReader reader, byte[] raw, ByteBuffer out, PathTrie<Boolean> trie) {
    int sizeOffset = out.position();
    @Var int spanStart = reader.getBsonInput().getPosition();

    // A path can only be excluded once per document, even if the document has repeated key values.
    Set<String> used = new HashSet<>(trie.getNumChildren());

    reader.readStartDocument();
    while (true) {
      int fieldOffset = reader.getBsonInput().getPosition();
      BsonType type = reader.readBsonType(); // Note: readBsonType() also reads name internally
      if (type == BsonType.END_OF_DOCUMENT) {
        break;
      }
      String fieldName = reader.readName();

      // Check if key is excluded, but only remove first occurrence if it is repeated.
      if (trie.isValidPrefix(fieldName) && used.add(fieldName)) {
        PathTrie<Boolean> child = trie.getChild(fieldName);
        boolean excludeEntireValue = child.getValue().orElse(false);

        switch (type) {
          case DOCUMENT -> {
            if (excludeEntireValue) {
              out.put(raw, spanStart, fieldOffset - spanStart);
              reader.skipValue();
              spanStart = reader.getBsonInput().getPosition();
            } else {
              out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
              copyWithExclusions(reader, raw, out, child);
              spanStart = reader.getBsonInput().getPosition();
            }
          }
          case ARRAY -> {
            if (excludeEntireValue) {
              out.put(raw, spanStart, fieldOffset - spanStart);
              reader.skipValue();
              spanStart = reader.getBsonInput().getPosition();
            } else {
              out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
              copyPartialArray(reader, raw, out, child);
              spanStart = reader.getBsonInput().getPosition();
            }
          }
          default -> {
            if (excludeEntireValue) {
              out.put(raw, spanStart, fieldOffset - spanStart);
              reader.skipValue();
              spanStart = reader.getBsonInput().getPosition();
            } else {
              reader.skipValue();
            }
          }
        }
      } else {
        reader.skipValue();
      }
    }
    reader.readEndDocument();
    out.put(raw, spanStart, reader.getBsonInput().getPosition() - spanStart);
    out.putInt(sizeOffset, out.position() - sizeOffset);
  }
}
