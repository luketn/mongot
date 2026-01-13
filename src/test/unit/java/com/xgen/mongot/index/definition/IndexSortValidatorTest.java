package com.xgen.mongot.index.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.Test;

public class IndexSortValidatorTest {

  @Test
  public void validateNoScoreFields_withRegularFields_passes() throws BsonParseException {
    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)
    ));

    // Should not throw
    IndexSortValidator.validateNoScoreFields(sort);
  }

  @Test
  public void validateNoScoreFields_withScoreField_throwsException() {
    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("scoreField"), 
            new MetaSortOptions(SortOrder.DESC, MetaSortField.SEARCH_SCORE))
    ));

    try {
      IndexSortValidator.validateNoScoreFields(sort);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Cannot sort on score for fields: [scoreField]", e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsStaticallyDefined_withDefinedFields_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    staticFields.put("field2", createTokenFieldDefinition());

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)
    ));

    // Should not throw
    IndexSortValidator.validateSortFieldsStaticallyDefined(sort, staticFields);
  }

  @Test
  public void validateSortFieldsStaticallyDefined_withUndefinedField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    // field2 is not defined

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)
    ));

    try {
      IndexSortValidator.validateSortFieldsStaticallyDefined(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Sort fields: [field2] are not statically defined", e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsHaveSingleType_withSingleTypeFields_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    staticFields.put("field2", createNumberFieldDefinition());

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)
    ));

    // Should not throw
    IndexSortValidator.validateSortFieldsHaveSingleType(sort, staticFields);
  }

  @Test
  public void validateSortFieldsHaveSingleType_withMultiTypeField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createMultiTypeFieldDefinition());

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)
    ));

    try {
      IndexSortValidator.validateSortFieldsHaveSingleType(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Sort fields: [field1] have mixed types or are not defined", e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsAreSortable_withSortableFields_passes() throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    staticFields.put("field2", createNumberFieldDefinition());

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
        new MongotSortField(FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)
    ));

    // Should not throw
    IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
  }

  @Test
  public void validateSortFieldsAreSortable_withNonSortableField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createStringFieldDefinition()); // String is not sortable

    Sort sort = new Sort(ImmutableList.of(
        new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)
    ));

    try {
      IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Sort fields: {field1=STRING} are not sortable types. Sortable types are: "
          + FieldDefinition.INDEXING_SORTABLE_TYPES, e.getMessage());
    }
  }

  // Helper methods to create field definitions
  private FieldDefinition createTokenFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createNumberFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
        .build();
  }

  private FieldDefinition createStringFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .string(StringFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createMultiTypeFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
        .build();
  }
}
