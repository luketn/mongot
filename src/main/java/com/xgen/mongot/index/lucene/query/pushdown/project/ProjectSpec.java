package com.xgen.mongot.index.lucene.query.pushdown.project;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.util.FieldPath;

@Immutable
public class ProjectSpec {

  /** A ProjectSpec that requires all fields to be returned. */
  public static final ProjectSpec PROJECT_ALL_STORED_SOURCE =
      new ProjectSpec(ImmutableList.of(), ImmutableList.of(), true);

  /**
   * A list of paths to either include or exclude. Path names are guaranteed to be unique and none
   * may be a parent or child of another specified path.
   */
  public final ImmutableList<FieldPath> pathsToInclude;

  public final ImmutableList<FieldPath> pathsToExclude;

  public final boolean returnStoredSource;

  ProjectSpec(
      ImmutableList<FieldPath> pathsToInclude,
      ImmutableList<FieldPath> pathsToExclude,
      boolean returnStoredSource) {
    this.pathsToInclude = pathsToInclude;
    this.pathsToExclude = pathsToExclude;
    this.returnStoredSource = returnStoredSource;
  }

  public ProjectSpec(boolean returnStoredSource, StoredSourceDefinition definition) {
    this.pathsToInclude = ImmutableList.of();
    this.pathsToExclude = ImmutableList.of();
    this.returnStoredSource = returnStoredSource;
  }
}
