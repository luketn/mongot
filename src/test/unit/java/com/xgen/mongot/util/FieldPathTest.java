package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.testing.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class FieldPathTest {

  @Test
  public void isDirectRelation() {
    assertTrue(FieldPath.parse("").isDirectRelation(FieldPath.parse("")));
    assertFalse(FieldPath.parse("").isDirectRelation(FieldPath.parse("a")));
    assertTrue(FieldPath.parse("").isDirectRelation(FieldPath.parse(".a")));

    // Function is symmetric
    assertTrue(FieldPath.parse("a").isDirectRelation(FieldPath.parse("a.b")));
    assertTrue(FieldPath.parse("a.b").isDirectRelation(FieldPath.parse("a")));

    // Function is reflective
    assertTrue(FieldPath.parse("a.b").isDirectRelation(FieldPath.parse("a.b")));

    // Siblings and cousins
    assertFalse(FieldPath.parse("a.b.c").isDirectRelation(FieldPath.parse("a.b.d")));
    assertFalse(FieldPath.parse("a.b.c").isDirectRelation(FieldPath.parse("a.x.c")));
    assertFalse(FieldPath.parse("a.b.c").isDirectRelation(FieldPath.parse("x")));
  }

  @Test
  public void testGetSegments() {
    assertThat(FieldPath.parse("").getSegments()).containsExactly("").inOrder();
    assertThat(FieldPath.parse("a").getSegments()).containsExactly("a").inOrder();
    assertThat(FieldPath.parse("a.b.c").getSegments()).containsExactly("a", "b", "c").inOrder();
  }

  @Test
  public void testFieldPath() {
    assertExpectedFieldPath("a", List.of("a"), Optional.empty());

    assertExpectedFieldPath("longFieldName", List.of("longFieldName"), Optional.empty());

    assertExpectedFieldPath("a.b.c", List.of("a", "b", "c"), Optional.of(FieldPath.parse("a.b")));

    assertExpectedFieldPath(".a", List.of("", "a"), Optional.of(FieldPath.parse("")));

    assertExpectedFieldPath(
        "a.b...", List.of("a", "b", "", "", ""), Optional.of(FieldPath.parse("a.b..")));

    assertExpectedFieldPath("a..b", List.of("a", "", "b"), Optional.of(FieldPath.parse("a.")));

    assertExpectedFieldPath(
        "a...b", List.of("a", "", "", "b"), Optional.of(FieldPath.parse("a..")));

    assertExpectedFieldPath(
        "a.b..c", List.of("a", "b", "", "c"), Optional.of(FieldPath.parse("a.b.")));

    assertExpectedFieldPath(
        "a. foo.bar", List.of("a", " foo", "bar"), Optional.of(FieldPath.parse("a. foo")));

    assertExpectedFieldPath(
        "a.foo .bar", List.of("a", "foo ", "bar"), Optional.of(FieldPath.parse("a.foo ")));
  }

  @Test
  public void testGetLeaf() {
    Assert.assertEquals("", FieldPath.parse("").getLeaf());
    Assert.assertEquals("a", FieldPath.parse("a").getLeaf());
    Assert.assertEquals("foo", FieldPath.parse("foo").getLeaf());
    Assert.assertEquals("bar", FieldPath.parse("foo.bar").getLeaf());
    Assert.assertEquals("baz", FieldPath.parse("foo.bar .baz").getLeaf());
  }

  @Test
  public void testGetChild() {
    FieldPath path = FieldPath.parse("a");

    @Var FieldPath child = path.newChild("b");
    Assert.assertEquals(child, FieldPath.parse("a.b"));

    child = child.newChild("foo.bar");
    Assert.assertEquals(child, FieldPath.parse("a.b.foo.bar"));
  }

  @Test
  public void testAncestorPaths() {
    assertExpectedAncestors(FieldPath.parse(""), List.of());
    assertExpectedAncestors(FieldPath.parse("a"), List.of());
    assertExpectedAncestors(FieldPath.parse("a.b"), List.of(FieldPath.parse("a")));
    assertExpectedAncestors(
        FieldPath.parse("a.b.foo.bar"),
        List.of(FieldPath.parse("a.b.foo"), FieldPath.parse("a.b"), FieldPath.parse("a")));
  }

  @Test
  public void testWithNewRoot() {
    FieldPath abc = FieldPath.parse("a.b.c");
    FieldPath zabc = abc.withNewRoot("z");

    Optional<FieldPath> zabcParent = zabc.getParent();
    assertTrue("zabc should have parent", zabcParent.isPresent());
    FieldPath zab = zabcParent.get();

    Optional<FieldPath> zabParent = zab.getParent();
    assertTrue("zab should have parent", zabParent.isPresent());
    FieldPath za = zabParent.get();

    Optional<FieldPath> zaParent = za.getParent();
    assertTrue("za should have parent", zaParent.isPresent());
    FieldPath z = zaParent.get();

    assertExpectedAncestors(z, List.of());
    assertExpectedAncestors(za, List.of(FieldPath.parse("z")));
    assertExpectedAncestors(zab, List.of(FieldPath.parse("z.a"), FieldPath.parse("z")));
    assertExpectedAncestors(
        zabc, List.of(FieldPath.parse("z.a.b"), FieldPath.parse("z.a"), FieldPath.parse("z")));

    Assert.assertEquals(4, zabc.getPathHierarchy().size());
    Assert.assertSame(z, zabc.getPathHierarchy().get(0));
    Assert.assertSame(za, zabc.getPathHierarchy().get(1));
    Assert.assertSame(zab, zabc.getPathHierarchy().get(2));
    Assert.assertSame(zabc, zabc.getPathHierarchy().get(3));
    assertTrue(zabc.isNested());

    Assert.assertEquals(3, zab.getPathHierarchy().size());
    Assert.assertSame(z, zab.getPathHierarchy().get(0));
    Assert.assertSame(za, zab.getPathHierarchy().get(1));
    Assert.assertSame(zab, zab.getPathHierarchy().get(2));
    assertTrue(zab.isNested());

    Assert.assertEquals(2, za.getPathHierarchy().size());
    Assert.assertSame(z, za.getPathHierarchy().get(0));
    Assert.assertSame(za, za.getPathHierarchy().get(1));
    assertTrue(za.isNested());

    Assert.assertEquals(1, z.getPathHierarchy().size());
    Assert.assertSame(z, z.getPathHierarchy().get(0));
    assertTrue(z.isAtRoot());
  }

  @Test
  public void testWithNewRootAtRoot() {
    FieldPath a = FieldPath.parse("a");
    FieldPath za = a.withNewRoot("z");

    Optional<FieldPath> zaParent = za.getParent();
    assertTrue("za should have parent", zaParent.isPresent());
    FieldPath z = zaParent.get();

    assertExpectedAncestors(z, List.of());
    assertExpectedAncestors(za, List.of(FieldPath.parse("z")));

    Assert.assertEquals(2, za.getPathHierarchy().size());
    Assert.assertSame(z, za.getPathHierarchy().get(0));
    Assert.assertSame(za, za.getPathHierarchy().get(1));
    assertTrue(za.isNested());

    Assert.assertEquals(1, z.getPathHierarchy().size());
    Assert.assertSame(z, z.getPathHierarchy().get(0));
    assertTrue(z.isAtRoot());
  }

  @Test
  public void testWithNewRootWithNonLeafRoot() {
    FieldPath a = FieldPath.parse("a");
    FieldPath zya = a.withNewRoot("z.y");

    Optional<FieldPath> zyaParent = zya.getParent();
    assertTrue("zya should have parent", zyaParent.isPresent());
    FieldPath zy = zyaParent.get();

    Optional<FieldPath> zyParent = zy.getParent();
    assertTrue("zy should have parent", zyParent.isPresent());
    FieldPath z = zyParent.get();

    assertExpectedAncestors(z, List.of());
    assertExpectedAncestors(zy, List.of(FieldPath.parse("z")));
    assertExpectedAncestors(zya, List.of(FieldPath.parse("z.y"), FieldPath.parse("z")));

    Assert.assertEquals(3, zya.getPathHierarchy().size());
    Assert.assertEquals(z, zya.getPathHierarchy().get(0));
    Assert.assertSame(zy, zya.getPathHierarchy().get(1));
    Assert.assertSame(zya, zya.getPathHierarchy().get(2));
    assertTrue(zya.isNested());

    Assert.assertEquals(2, zy.getPathHierarchy().size());
    Assert.assertEquals(z, zy.getPathHierarchy().get(0));
    Assert.assertSame(zy, zy.getPathHierarchy().get(1));
    assertTrue(zy.isNested());

    Assert.assertEquals(1, z.getPathHierarchy().size());
    Assert.assertSame(z, z.getPathHierarchy().get(0));
    assertTrue(z.isAtRoot());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> FieldPath.parse(""),
        () -> FieldPath.parse("a"),
        () -> FieldPath.parse("a.b"),
        () -> FieldPath.parse("ab"),
        () -> FieldPath.parse("a.b.foo.bar"),
        () -> new String("a"));
  }

  @Test
  public void testIsChildOf() {
    FieldPath parent = FieldPath.parse("a");
    FieldPath child = FieldPath.parse("a.b");
    assertTrue(child.isChildOf(parent));

    FieldPath childWithWildcard = FieldPath.parse("a.*");
    assertTrue(childWithWildcard.isChildOf(parent));

    FieldPath invalidChild = FieldPath.parse("a*");
    assertFalse(invalidChild.isChildOf(parent));

    assertFalse(parent.isChildOf(parent));
  }

  private static void assertExpectedFieldPath(
      String path, List<String> expectedParts, Optional<FieldPath> expectedParent) {
    FieldPath fieldPath = FieldPath.parse(path);
    Assert.assertEquals(path, fieldPath.toString());
    Assert.assertEquals(
        expectedParts,
        fieldPath.getPathHierarchy().stream().map(FieldPath::getLeaf).collect(Collectors.toList()));
    Assert.assertEquals(expectedParent.isEmpty(), fieldPath.isAtRoot());
    Assert.assertEquals(expectedParent.isPresent(), fieldPath.isNested());
    Assert.assertEquals(expectedParent.isPresent(), fieldPath.isNested());
    Assert.assertEquals(expectedParent, fieldPath.getParent());
  }

  private static void assertExpectedAncestors(FieldPath path, List<FieldPath> expected) {
    List<FieldPath> ancestors = path.ancestorPaths().collect(Collectors.toList());
    Assert.assertEquals("paths did not match", expected, ancestors);
  }
}
