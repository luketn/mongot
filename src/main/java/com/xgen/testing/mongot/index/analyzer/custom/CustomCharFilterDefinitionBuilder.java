package com.xgen.testing.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.custom.HtmlStripCharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.IcuNormalizeCharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.MappingCharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.PersianCharFilterDefinition;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class CustomCharFilterDefinitionBuilder {
  public static class HtmlStripCharFilter {
    Set<String> ignoredTags = new HashSet<>();

    public HtmlStripCharFilter ignoredTag(String tag) {
      this.ignoredTags.add(tag);
      return this;
    }

    public static HtmlStripCharFilter builder() {
      return new HtmlStripCharFilter();
    }

    public HtmlStripCharFilterDefinition build() {
      return new HtmlStripCharFilterDefinition(this.ignoredTags);
    }
  }

  public static class IcuNormalizeCharFilter {
    public static IcuNormalizeCharFilterDefinition build() {
      return new IcuNormalizeCharFilterDefinition();
    }
  }

  public static class MappingCharFilter {
    HashMap<String, String> mappings = new HashMap<>();

    public MappingCharFilter mapping(String key, String value) {
      this.mappings.put(key, value);
      return this;
    }

    public static MappingCharFilter builder() {
      return new MappingCharFilter();
    }

    public MappingCharFilterDefinition build() {
      return new MappingCharFilterDefinition(this.mappings);
    }
  }

  public static class PersianCharFilter {
    public static PersianCharFilterDefinition build() {
      return new PersianCharFilterDefinition();
    }
  }
}
