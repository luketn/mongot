package com.xgen.testing.mongot.index.synonym;

import com.xgen.mongot.index.synonym.SynonymDocument;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public class SynonymDocumentBuilder {
  public static SynonymDocument explicit(
      List<String> input, List<String> synonyms, BsonValue docId) {
    return SynonymDocument.create(
        SynonymDocument.MappingType.EXPLICIT,
        synonyms,
        Optional.of(input),
        SynonymDocument.idStringFromBsonValue(Optional.of(docId)));
  }

  public static SynonymDocument explicit(List<String> input, List<String> synonyms) {
    return SynonymDocument.create(
        SynonymDocument.MappingType.EXPLICIT, synonyms, Optional.of(input), Optional.empty());
  }

  public static SynonymDocument equivalent(List<String> synonyms, BsonValue docId) {
    return SynonymDocument.create(
        SynonymDocument.MappingType.EQUIVALENT,
        synonyms,
        Optional.empty(),
        SynonymDocument.idStringFromBsonValue(Optional.of(docId)));
  }

  public static SynonymDocument equivalent(List<String> synonyms) {
    return SynonymDocument.create(
        SynonymDocument.MappingType.EQUIVALENT, synonyms, Optional.empty(), Optional.empty());
  }
}
