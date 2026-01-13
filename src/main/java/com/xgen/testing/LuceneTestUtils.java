package com.xgen.testing;

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;

public final class LuceneTestUtils {

  private LuceneTestUtils() {
    // Utility class
  }

  /** An IndexableField wrapper that defines equals and hashcode. */
  static final class FieldProxy extends Field {
    private final Object value;

    private final String stringRep;

    FieldProxy(Field f) {
      super(f.name(), f.fieldType());
      this.value =
          ObjectUtils.firstNonNull(f.binaryValue(), f.getCharSequenceValue(), f.numericValue());
      this.stringRep = f.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldProxy that = (FieldProxy) o;
      return Objects.equal(this.type, that.type)
          && Objects.equal(this.value, that.value)
          && Objects.equal(this.stringRep, that.stringRep);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.type, this.value, this.stringRep);
    }
  }

  /** Helper method that constructs a {@link Document} containing all the given fields. */
  public static Document document(IndexableField... fields) {
    Document d = new Document();
    for (IndexableField field : fields) {
      d.add(field);
    }
    return d;
  }

  /** Sort fields deterministically (preferably by field name) before comparing elements. */
  private static List<IndexableField> sort(Document d) {
    return d.getFields().stream()
        .sorted(Comparator.comparing(IndexableField::name).thenComparing(IndexableField::toString))
        .collect(toList());
  }

  /**
   * Asserts that two {@link Document}s are equal (ignoring field order).
   *
   * <p>This method works by comparing the string representation for of each indexed field because
   * most of the built-in {@link IndexableField} subclasses do not implement {@link
   * Object#equals(Object)}, but they do implement {@link Object#toString()}.
   */
  public static void assertFieldsEquals(Document expected, Document actual) {
    var expectedFields = sort(expected);
    var expectedStrings = Lists.transform(expectedFields, Object::toString);

    var actualFields = sort(actual);
    var actualStrings = Lists.transform(actualFields, Object::toString);
    assertThat(actualStrings).containsExactlyElementsIn(expectedStrings).inOrder();

    var expectedProxy = Lists.transform(sort(expected), x -> new FieldProxy((Field) x));
    var actualProxy = Lists.transform(sort(actual), x -> new FieldProxy((Field) x));
    assertThat(actualProxy).containsExactlyElementsIn(expectedProxy).inOrder();
  }
}
