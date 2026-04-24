package com.xgen.mongot.embedding.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

/** Unit tests for {@link DefaultInternalDatabaseResolver}. */
public class InternalDatabaseResolverTest {

  @Test
  public void resolve_alwaysReturnsBaseName_regardlessOfSourceDatabase() {
    var resolver = new DefaultInternalDatabaseResolver();
    assertThat(resolver.resolve("acme_mydb")).isEqualTo("__mdb_internal_search");
    assertThat(resolver.resolve("mydb")).isEqualTo("__mdb_internal_search");
    assertThat(resolver.resolve("tenant123_analytics")).isEqualTo("__mdb_internal_search");
  }

  @Test
  public void resolve_customBaseName_returnsCustomBaseName() {
    var resolver = new DefaultInternalDatabaseResolver("test_db");
    assertThat(resolver.resolve("acme_mydb")).isEqualTo("test_db");
  }

  @Test
  public void resolveDefault_returnsBaseName() {
    assertThat(new DefaultInternalDatabaseResolver().resolveDefault())
        .isEqualTo("__mdb_internal_search");
  }

  @Test
  public void resolveDefault_customBaseName_returnsCustomBaseName() {
    assertThat(new DefaultInternalDatabaseResolver("custom_db").resolveDefault())
        .isEqualTo("custom_db");
  }

  @Test
  public void defaultBaseName_returnsExpectedValue() {
    assertThat(InternalDatabaseResolver.defaultBaseName()).isEqualTo("__mdb_internal_search");
  }
}
