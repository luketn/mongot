package com.xgen.mongot.index.query.sort;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * SortSpec is the parent class of the concrete Sort and SortBetaV1 query syntax classes.This class
 * enforces that the same core functionality is present in both implementations of the SortSpec.
 */
public sealed interface SortSpec extends DocumentEncodable permits Sort, SortBetaV1 {
  /**
   * Creates a new {@link SortSpec} that will sort documents in the reverse order as this original
   * SortSpec. <br>
   * <br>
   * For example, if this SortSpec yields the ordering [A, B, C], then {@code this.invert()} should
   * always yield the ordering of [C, B, A] irrespective of field types, repeated fields, embedded
   * documents, or missing values.
   */
  SortSpec invert();

  ImmutableList<MongotSortField> getSortFields();

  static ImmutableList<MongotSortField> getSortFieldsFromBson(
      BsonParseContext context, BsonValue value) throws BsonParseException {
    if (!value.isDocument()) {
      return context.handleUnexpectedType("document", value.getBsonType());
    }

    var document = value.asDocument();

    // The ordering of fields in the sort specification are semantically important, defining the
    // compound sort order. The iterator returned by BsonDocument::entrySet honors that order by
    // being backed by a LinkedHashSet.
    var fields =
        CheckedStream.fromSequential(document.entrySet())
            .mapAndCollectChecked(
                entry -> MongotSortField.fromBson(entry.getKey(), entry.getValue(), context));
    if (fields.isEmpty()) {
      return context.handleSemanticError("Sort spec must have at least one sort field defined");
    }

    return ImmutableList.copyOf(fields);
  }

  @Override
  default BsonDocument toBson() {
    var fields = getSortFields();
    var document = new BsonDocument(fields.size());
    fields.forEach(spec -> document.append(spec.field().toString(), spec.options().toBson()));
    return document;
  }
}
