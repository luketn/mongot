package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class CollectionUtilsTest {

  @Test
  public void testFindDuplicates() {
    Set<Integer> uniqueInput = CollectionUtils.findDuplicates(List.of(1, 2, 3));
    Assert.assertEquals(uniqueInput, Collections.emptySet());

    Set<Integer> emptyInput = CollectionUtils.findDuplicates(Collections.emptyList());
    Assert.assertEquals(emptyInput, Collections.emptySet());

    Set<Integer> oneIsDuplicate = CollectionUtils.findDuplicates(List.of(1, 2, 1));
    Assert.assertEquals(oneIsDuplicate, Set.of(1));
  }

  @Test
  public void testConcat() {
    Assert.assertEquals(List.of("a", "b"), CollectionUtils.concat(List.of("a"), List.of("b")));
    Assert.assertEquals(
        List.of("a", "a", "b"), CollectionUtils.concat(List.of("a", "a"), List.of("b"), List.of()));
    Assert.assertEquals(
        List.of("a", "b", "c", "d"),
        CollectionUtils.concat(List.of("a"), List.of("b"), List.of("c", "d")));
  }

  @Test
  public void testAppend() {
    Assert.assertEquals(List.of("a", "b"), CollectionUtils.append(List.of("a"), "b"));
    Assert.assertEquals(List.of("a", "a", "b"), CollectionUtils.append(List.of("a", "a"), "b"));
    Assert.assertEquals(List.of("a"), CollectionUtils.append(List.of(), "a"));
  }

  @Test
  public void bsonArrayCollector() {
    BsonArray expected =
        new BsonArray(List.of(BsonUtils.MAX_KEY, BsonUtils.MIN_KEY, new BsonDocument()));

    BsonArray actual =
        Stream.of(BsonUtils.MAX_KEY, BsonUtils.MIN_KEY, new BsonDocument())
            .collect(CollectionUtils.toBsonArray());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNewMapFromKeys() {
    Map<String, Integer> input =
        Map.of(
            "a", 1,
            "b", 2,
            "c", 3);

    Map<String, String> output = CollectionUtils.newMapFromKeys(input, (key, value) -> key + key);

    assertEquals(
        Map.of(
            "a", "aa",
            "b", "bb",
            "c", "cc"),
        output);
  }
}
