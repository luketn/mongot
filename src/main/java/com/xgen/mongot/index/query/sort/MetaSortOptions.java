package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import java.util.Optional;
import org.apache.lucene.search.SortField;
import org.bson.BsonDocument;

/**
 * This class encapsulates options for sorting based on metadata fields rather than document
 * content. It extends SortOptions to provide specific behavior for sorting meta fields in Lucene.
 */
public record MetaSortOptions(SortOrder order, SortSelector selector, MetaSortField meta)
    implements SortOptions {

  /**
   * Indicates that we should sort by a meta field rather than the field matching the name of the
   * key that mapped to these SortOptions.
   */
  public MetaSortOptions(SortOrder order, MetaSortField meta) {
    this(order, order == SortOrder.ASC ? SortSelector.MIN : SortSelector.MAX, meta);
  }

  /**
   * Returns a new {@link MetaSortOptions} object that has all the same settings, except it has an
   * inverted {@link SortOrder}.
   *
   * <p>Note: The {@link SortSelector} is preserved in the new object even though its default value
   * is usually determined by the SortOrder. That is, {@code DEFAULT_ASC.invert() != DEFAULT_DESC}
   * because {@link #DEFAULT_ASC} and {@code DEFAULT_ASC.invert()} both use {@link
   * SortSelector#MIN}.
   */
  @Override
  public MetaSortOptions invert() {
    return new MetaSortOptions(this.order.invert(), this.selector, this.meta);
  }

  /** Construct a {@link SortField} to represent the requested meta sort. */
  public SortField getMetaSort() {
    return this.meta.getSortField(this);
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder().field(Fields.ORDER, Optional.of(this.order));
    builder.field(Fields.META, Optional.of(this.meta));
    return builder.build();
  }

  @Override
  public String toString() {
    return "MetaSortOptions(" + "meta=" + this.meta + ')';
  }
}
