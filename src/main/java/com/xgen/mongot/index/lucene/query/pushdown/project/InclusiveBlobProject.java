package com.xgen.mongot.index.lucene.query.pushdown.project;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.util.HashSet;
import java.util.Set;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

/**
 * A ProjectStrategy that excludes all fields in an input doc except those explicitly included. This
 * is implemented by reading the original document as a blob from a single column. There are a few
 * reasons to do this:
 *
 * <ol>
 *   <li>It's not possible to construct the project from individual columns (e.g. one of the fields
 *       refers to a document, which we don't index separately).
 *   <li>We're projecting a lot of fields of a small document, and therefore splicing multiple
 *       columns together has more overhead than parsing the full document.
 * </ol>
 */
class InclusiveBlobProject implements ProjectionTransform<RawBsonDocument, RawBsonDocument> {

  private static final DecoderContext DEFAULT_CONTEXT = DecoderContext.builder().build();
  private final PathTrie<Boolean> trie;

  /**
   * Creates a {@link ProjectStrategy} that reads documents from `blobStorage` and producing fields
   * specified by `spec`
   *
   * @param spec - A list of fields to include. {@code spec.isInclusive} must be true.
   */
  InclusiveBlobProject(ProjectSpec spec) {
    checkArg(!spec.pathsToInclude.isEmpty(), "Projection must be inclusive");
    this.trie = new PathTrie<>();
    for (FieldPath path : spec.pathsToInclude) {
      this.trie.put(path, true);
    }
  }

  @Override
  public RawBsonDocument project(RawBsonDocument document) {
    ByteBuf src = document.getByteBuffer();
    BasicOutputBuffer output = new BasicOutputBuffer(src.remaining());
    BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(src));
    BsonBinaryWriter writer = new BsonBinaryWriter(output);

    copyPartialDocument(reader, writer, this.trie);
    return new RawBsonDocument(output.toByteArray());
  }

  /**
   * Appends the next value from `reader` to the `writer` with the name `field`.
   *
   * <p>The reader should be in the `VALUE` state, and writer should be in the `NAME` state.
   */
  private static <T extends BsonValue> void copyValue(
      BsonBinaryReader reader, BsonBinaryWriter writer, Codec<T> codec, String field) {
    T value = codec.decode(reader, DEFAULT_CONTEXT);
    writer.writeName(field);
    codec.encode(writer, value, BsonUtils.DEFAULT_FAST_CONTEXT);
  }

  /**
   * Reads an array from `reader` and appends a array to `writer` by copying partial documents from
   * its values according to `trie`. If there are no matching values to copy, an empty array is
   * appended.
   *
   * @param reader - a BsonReader positioned at the start of an array element
   * @param writer - a writer positioned ready to write a new bson value
   * @param trie - a set of relative paths to include in the output
   */
  private static void copyPartialArray(
      BsonBinaryReader reader, BsonBinaryWriter writer, PathTrie<Boolean> trie) {
    writer.writeStartArray();
    reader.readStartArray();

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.getCurrentBsonType()) {
        case ARRAY ->
            // Unlike $match, we expand any number of nested arrays
            copyPartialArray(reader, writer, trie);
        case DOCUMENT ->
            // $project paths don't include array indices, so we just forward the call to the
            // document
            copyPartialDocument(reader, writer, trie);
        default ->
            // Leaf values can't contain paths, so we can just skip this value
            reader.skipValue();
      }
    }
    reader.readEndArray();
    writer.writeEndArray();
  }

  /**
   * Reads a document from `reader` and appends a document to `writer` with paths specified by
   * `trie`. If there are no matching paths, an empty document is appended.
   *
   * @param reader - a BsonReader positioned at the start of a new document
   * @param writer - a BsonWriter ready to write a new value
   * @param trie - a set of relative paths to include in the new document
   */
  private static void copyPartialDocument(
      BsonBinaryReader reader, BsonBinaryWriter writer, PathTrie<Boolean> trie) {
    reader.readStartDocument();
    writer.writeStartDocument();
    // Invariant: reader is at start of document or array
    Set<String> used = new HashSet<>(trie.getNumChildren());

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String field = reader.readName();

      if (trie.isValidPrefix(field) && used.add(field)) {
        PathTrie<Boolean> child = trie.getChild(field);
        boolean includeEntireValue = child.getValue().orElse(false);

        switch (reader.getCurrentBsonType()) {
          case DOCUMENT -> {
            if (includeEntireValue) {
              copyValue(reader, writer, BsonUtils.BSON_DOCUMENT_CODEC, field);
            } else {
              writer.writeName(field);
              copyPartialDocument(reader, writer, child);
            }
          }
          case ARRAY -> {
            if (includeEntireValue) {
              copyValue(reader, writer, BsonUtils.BSON_ARRAY_CODEC, field);
            } else {
              writer.writeName(field);
              copyPartialArray(reader, writer, child);
            }
          }
          default -> {
            if (includeEntireValue) {
              copyValue(reader, writer, BsonUtils.BSON_VALUE_CODEC, field);
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
    writer.writeEndDocument();
  }
}
