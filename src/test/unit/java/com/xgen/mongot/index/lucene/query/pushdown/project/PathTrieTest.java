package com.xgen.mongot.index.lucene.query.pushdown.project;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import org.junit.Test;

public class PathTrieTest {

  @Test
  public void put() {
    PathTrie<String> trie = new PathTrie<>();
    trie.put(FieldPath.parse("a.b"), "ab");
    trie.put(FieldPath.parse("a.b.c"), "abc");
    trie.put(FieldPath.parse("a.c"), "ac");
    trie.put(FieldPath.parse("b"), "b");

    assertEquals(2, trie.getNumChildren());
    assertThat(trie.get(FieldPath.parse("a.b"))).hasValue("ab");
  }

  @Test
  public void getChild() {
    PathTrie<String> trie = new PathTrie<>();
    trie.put(FieldPath.parse("a.b"), "ab");
    trie.put(FieldPath.parse("a.b.c"), "abc");
    trie.put(FieldPath.parse("a.c"), "ac");
    trie.put(FieldPath.parse("b"), "b");

    assertEquals(2, trie.getChild("a").getNumChildren());
    assertThat(trie.getChild("a").getChild("b").getValue()).hasValue("ab");
  }

  @Test
  public void isValidPrefix() {
    PathTrie<String> trie = new PathTrie<>();
    trie.put(FieldPath.parse("a.b"), "ab");
    trie.put(FieldPath.parse("a.b.c"), "abc");
    trie.put(FieldPath.parse("a.c"), "ac");
    trie.put(FieldPath.parse("b"), "b");

    assertTrue(trie.isValidPrefix("a"));
    assertTrue(trie.isValidPrefix(FieldPath.parse("a")));
    assertTrue(trie.isValidPrefix(FieldPath.parse("a.b")));
    assertTrue(trie.isValidPrefix(FieldPath.parse("a.b.c")));
  }

  @Test
  public void isNotValidPrefix() {
    PathTrie<String> trie = new PathTrie<>();
    trie.put(FieldPath.parse("a.b"), "ab");
    trie.put(FieldPath.parse("a.b.c"), "abc");
    trie.put(FieldPath.parse("a.c"), "ac");
    trie.put(FieldPath.parse("b"), "b");

    assertFalse(trie.isValidPrefix("z"));
    assertFalse(trie.isValidPrefix(FieldPath.parse("z")));
    assertFalse(trie.isValidPrefix(FieldPath.parse("a.b.c.d")));
  }
}
