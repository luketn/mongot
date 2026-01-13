package com.xgen.mongot.index;

import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.synonym.SynonymRegistry;

public interface SearchIndex extends Index {

  @Override
  SearchIndexDefinition getDefinition();

  /** Gets the synonym registry for this index. */
  SynonymRegistry getSynonymRegistry();
}
