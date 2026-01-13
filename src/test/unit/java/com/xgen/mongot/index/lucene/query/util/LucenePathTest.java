package com.xgen.mongot.index.lucene.query.util;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringMultiFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LucenePathTest {
  private static Directory directory;
  private static IndexWriter writer;
  private int counter;

  /** set up an index. */
  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    this.counter = 0;
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  @Test
  public void testPathMatchesAll() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.b");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("a.*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.b")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testPathContainsRegexEndQuote() throws IOException {
    // Weird FieldPaths like this break doing efficient Matcher#appendReplacement
    List<String> toIndex = List.of("\\Ea");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("\\Ea.*"));
    List<StringPath> result = List.of();
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testPathMatchSome() throws IOException {
    List<String> toIndex = List.of("a", "a.b.c", "b.c.d", "a.*", "a..");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("a.*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.*")),
            new StringFieldPath(FieldPath.parse("a..")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testPathWildcardPosition1() throws IOException {
    List<String> toIndex = List.of("a.b.c", "b.c.d");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("*.c"));
    List<StringPath> result = List.of(new StringFieldPath(FieldPath.parse("a.b.c")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testPathWildcardPosition2() throws IOException {
    List<String> toIndex = List.of("a.b.c", "b.c.d", "b.c.d.e");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("*.c.*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("b.c.d")),
            new StringFieldPath(FieldPath.parse("b.c.d.e")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testPathWildcardPosition3() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.d.c");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("a.*.c"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.d.c")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardEdge1() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.d.c");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.d.c")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardEdge2() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.d.c");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("*.*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.d.c")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardsDoNotMatchMulti() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.b");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("a.*"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.b")));
    Document doc = new Document();
    doc.add(
        new TextField(
            FieldName.MultiField.getLuceneFieldName(
                new StringMultiFieldPath(FieldPath.parse("a.b.c.d"), "sampleMulti"),
                Optional.empty()),
            "_",
            Field.Store.NO));
    index(List.of(doc));

    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardsDoNotMatchFacet() throws IOException {
    List<String> toIndex = List.of("a.b");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("*"));
    List<StringPath> result = List.of(new StringFieldPath(FieldPath.parse("a.b")));

    Document doc = new Document();
    doc.add(
        new StringField(
            FieldName.StaticField.FACET.getLuceneFieldName(), "drilldown-value", Field.Store.NO));
    doc.add(
        new SortedSetDocValuesField(
            FieldName.StaticField.FACET.getLuceneFieldName(), new BytesRef("count-value")));
    index(List.of(doc));

    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardAndFieldPath() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.b");
    List<UnresolvedStringPath> paths =
        List.of(
            new UnresolvedStringWildcardPath("a.*"),
            new UnresolvedStringFieldPath(FieldPath.parse("a.b.c.d")));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.b")),
            new StringFieldPath(FieldPath.parse("a.b.c.d")));
    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testWildcardAndFieldPathDuplicated() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.b", "a.b.c.d");
    List<UnresolvedStringPath> paths =
        List.of(
            new UnresolvedStringWildcardPath("a.*"),
            new UnresolvedStringFieldPath(FieldPath.parse("a.b.c.d")));
    List<StringFieldPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c.d")),
            new StringFieldPath(FieldPath.parse("a.b")),
            new StringFieldPath(FieldPath.parse("a.b.c")));

    indexStringDocs(toIndex);
    IndexReader indexReader = DirectoryReader.open(directory);
    Assert.assertEquals(
        result, LucenePath.resolveStringPaths(indexReader, paths, Optional.empty()));
  }

  @Test
  public void testStringPathBasic() throws IOException {
    List<String> toIndex = List.of("a.b.c", "a.b");
    List<UnresolvedStringPath> paths =
        List.of(
            new UnresolvedStringWildcardPath("a.*"),
            new UnresolvedStringMultiFieldPath(FieldPath.parse("a.b.c.d"), "some-multi"));
    List<StringPath> result =
        List.of(
            new StringFieldPath(FieldPath.parse("a.b.c")),
            new StringFieldPath(FieldPath.parse("a.b")),
            new StringMultiFieldPath(FieldPath.parse("a.b.c.d"), "some-multi"));

    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testRegexEscape() throws Exception {
    List<String> toIndex = List.of("a.b.c");
    List<UnresolvedStringPath> paths = List.of(new UnresolvedStringWildcardPath("....*"));

    List<StringPath> result = List.of();

    runStringPathTest(toIndex, paths, result);
  }

  @Test
  public void testDeDuplicateResolveWildcardPath() throws Exception {
    List<String> toIndex = List.of("ab");
    List<UnresolvedStringWildcardPath> paths =
        List.of(
            UnresolvedStringPathBuilder.wildcardPath("a*"),
            UnresolvedStringPathBuilder.wildcardPath("*b"));

    List<StringPath> result = List.of(StringPathBuilder.fieldPath("ab"));

    indexStringDocs(toIndex);
    IndexReader indexReader = DirectoryReader.open(directory);
    List<StringPath> resolvedWildcardPaths =
        LucenePath.resolveWildcardPaths(indexReader, paths, Optional.empty())
            .collect(Collectors.toUnmodifiableList());
    checkDuplicatesAndElements(resolvedWildcardPaths, result);
  }

  private void runStringPathTest(
      List<String> toIndex, List<UnresolvedStringPath> paths, List<StringPath> result)
      throws IOException {
    indexStringDocs(toIndex);

    IndexReader indexReader = DirectoryReader.open(directory);
    List<StringPath> resolvedStringPaths =
        LucenePath.resolveStringPaths(indexReader, paths, Optional.empty());
    checkDuplicatesAndElements(resolvedStringPaths, result);
  }

  private void checkDuplicatesAndElements(List<StringPath> expected, List<StringPath> actual) {
    Assert.assertEquals(expected.size(), actual.size());

    var expectedSet = new HashSet<>(expected);
    var actualSet = new HashSet<>();

    for (StringPath resolvedPath : actual) {
      Assert.assertFalse(actualSet.contains(resolvedPath));
      actualSet.add(resolvedPath);
    }
    Assert.assertEquals(expectedSet, actualSet);
  }

  private void indexStringDocs(List<String> toIndex) throws IOException {
    var docs = toIndex.stream().map(this::stringFieldDocument).collect(Collectors.toList());
    index(docs);
  }

  private void index(List<Document> docs) throws IOException {
    for (Document doc : docs) {
      int id = this.counter++;
      doc.add(new StoredField(FieldName.MetaField.ID.getLuceneFieldName(), id));
      writer.addDocument(doc);
    }

    writer.commit();
  }

  private Document stringFieldDocument(String field) {
    Document doc = new Document();
    doc.add(
        new TextField(
            FieldName.TypeField.STRING.getLuceneFieldName(FieldPath.parse(field), Optional.empty()),
            "_",
            Field.Store.NO));
    return doc;
  }
}
