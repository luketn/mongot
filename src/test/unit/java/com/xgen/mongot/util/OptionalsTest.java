package com.xgen.mongot.util;

import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class OptionalsTest {

  @Test
  public void orElseThrow() {
    int x = Optionals.orElseThrow(Optional.of(5), "Do not throw");
    Assert.assertEquals(5, x);

    Exception e =
        assertThrows(
            NoSuchElementException.class,
            () -> Optionals.orElseThrow(Optional.empty(), "Throw me"));
    Assert.assertEquals("Throw me", e.getMessage());
  }

  @Test
  public void testPresent() {
    Assert.assertEquals(List.of(), Optionals.present(Stream.of()).collect(Collectors.toList()));

    Assert.assertEquals(
        List.of(), Optionals.present(Stream.of(Optional.empty())).collect(Collectors.toList()));

    Assert.assertEquals(
        List.of(1, 2),
        Optionals.present(Stream.of(Optional.of(1), Optional.of(2))).collect(Collectors.toList()));

    Assert.assertEquals(
        List.of(1, 3),
        Optionals.present(Stream.of(Optional.of(1), Optional.empty(), Optional.of(3)))
            .collect(Collectors.toList()));
  }
}
