package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;
import java.util.Objects;
import java.util.Optional;
import javax.net.ssl.SSLContext;

public class SyncSourceConfig {

  public final ConnectionString mongodUri;
  public final Optional<ConnectionString> mongosUri;
  public final ConnectionString mongodClusterUri;
  public final Optional<SSLContext> sslContext;

  public SyncSourceConfig(
      ConnectionString mongodUri,
      Optional<ConnectionString> mongosUri,
      ConnectionString mongodClusterUri) {
    this.mongodUri = mongodUri;
    this.mongosUri = mongosUri;
    this.mongodClusterUri = mongodClusterUri;
    this.sslContext = Optional.empty();
  }

  public SyncSourceConfig(
      ConnectionString mongodUri,
      Optional<ConnectionString> mongosUri,
      ConnectionString mongodClusterUri,
      Optional<SSLContext> sslContext) {
    this.mongodUri = mongodUri;
    this.mongosUri = mongosUri;
    this.mongodClusterUri = mongodClusterUri;
    this.sslContext = sslContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SyncSourceConfig that = (SyncSourceConfig) o;
    return this.mongodUri.equals(that.mongodUri)
        && this.mongosUri.equals(that.mongosUri)
        && this.mongodClusterUri.equals(that.mongodClusterUri)
        && this.sslContext.equals(that.sslContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.mongodUri, this.mongosUri, this.mongodClusterUri, this.sslContext);
  }
}
