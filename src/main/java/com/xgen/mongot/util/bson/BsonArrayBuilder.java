package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.Bytes;
import org.bson.BsonArray;
import org.bson.RawBsonDocument;

/**
 * Builds {@link BsonArray}, making sure that total size stays within the specified limit. All sizes
 * are calculated assuming that strings are UTF-8 encoded. See the BSON spec for details:
 * https://bsonspec.org/spec.html
 */
public class BsonArrayBuilder {

  /** Includes 4 bytes for encoded array size and 1 terminating byte. */
  private static final Bytes ARRAY_OVERHEAD_BYTES = Bytes.ofBytes(5);

  /** Each BSON array key includes 1 byte representing its data type and 1 terminating byte. */
  private static final Bytes ARRAY_KEY_OVERHEAD_BYTES = Bytes.ofBytes(2);

  private final BsonArray array;
  private final Bytes limit;
  private Bytes total;

  private BsonArrayBuilder(Bytes limit) {
    this.limit = limit;
    this.total = ARRAY_OVERHEAD_BYTES;
    this.array = new BsonArray();
  }

  public static BsonArrayBuilder withLimit(Bytes limit) {
    return new BsonArrayBuilder(limit);
  }

  public static BsonArrayBuilder unlimited() {
    return new BsonArrayBuilder(Bytes.ofGibi(Long.MAX_VALUE));
  }

  /**
   * Appends the provided element if it could fit into the fixed size array.
   *
   * @return true if the element is appended, false otherwise.
   */
  public boolean append(RawBsonDocument element) {

    var nextElementSize = getNextElementSize(element);

    if (this.total.add(nextElementSize).compareTo(this.limit) > 0) {
      return false;
    }

    this.total = this.total.add(nextElementSize);
    this.array.add(element);

    return true;
  }

  public BsonArray build() {
    return this.array;
  }

  public int getDocumentCount() {
    return this.array.size();
  }

  public Bytes getDataSize() {
    return this.total;
  }

  /**
   * BsonArray encoded as a regular document, where each key is a string representation of the
   * element's ordinal array position. We are taking this into account to calculate the total size
   * precisely.
   */
  private Bytes getNextElementSize(RawBsonDocument element) {
    var elementSize = Bytes.ofBytes(element.getByteBuffer().remaining());
    return elementSize.add(getNextKeySize());
  }

  /** Returns the size of the next key ordinal number in UTF-8 string representation. */
  private Bytes getNextKeySize() {

    var keySize =
        this.array.isEmpty()
            ? Bytes.ofBytes(1)
            : Bytes.ofBytes((int) Math.log10(this.array.size()) + 1);

    return keySize.add(ARRAY_KEY_OVERHEAD_BYTES);
  }
}
