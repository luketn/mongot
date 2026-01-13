package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.config.LuceneConfig;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneGlobalSettings {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneGlobalSettings.class);

  public static void apply(LuceneConfig config) {
    if (config.disableMaxClauseLimit()) {
      IndexSearcher.setMaxClauseCount(Integer.MAX_VALUE);
    }
    LOG.atInfo()
        .addKeyValue("maxClauseCount", IndexSearcher.getMaxClauseCount())
        .log("Lucene max clause count set");
  }
}
