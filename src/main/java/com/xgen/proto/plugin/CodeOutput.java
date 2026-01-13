package com.xgen.proto.plugin;

import static com.google.common.base.Preconditions.checkState;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.EnumDescriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.Optional;

/**
 * Utility class to manage a block of generated code.
 *
 * <p>Use openScope() to begin a new scope in which you can emit arbitrary lines of code or create
 * child scopes. Each scope is a closeable resource that automatically handles indentation.
 * CodeOutput and any child scopes provided may each have only one child scope at a time and only
 * the innermost scope may be written to; this invariant is enforced at runtime.
 */
class CodeOutput {
  private final StringBuilder buf = new StringBuilder();
  private final Scope rootScope;

  CodeOutput() {
    this.rootScope = new Scope(this);
  }

  private void writeLine(String line) {
    this.buf.append(line);
    this.buf.append(System.lineSeparator());
  }

  public Scope getRootScope() {
    return this.rootScope;
  }

  @MustBeClosed
  public Scope openScope(String expr) {
    return getRootScope().openScope(expr);
  }

  @FormatMethod
  @MustBeClosed
  public Scope openScope(String format, Object... args) {
    return getRootScope().openScope(format, args);
  }

  /** A Scope represents an indented block between two curly brackets. */
  public static class Scope implements AutoCloseable {
    private static final int INDENT_WIDTH = 2;
    private final CodeOutput root;
    private final Optional<Scope> parent;
    private final int indent;
    private boolean hasOpenScope = false;

    private Scope(CodeOutput codeOutput) {
      this.root = codeOutput;
      this.parent = Optional.empty();
      this.indent = 0;
    }

    private Scope(Scope parent) {
      this.root = parent.root;
      if (parent.hasOpenScope) {
        throw new IllegalStateException("Scope already has an open child scope");
      }
      parent.hasOpenScope = true;
      this.parent = Optional.of(parent);
      this.indent = parent.indent + INDENT_WIDTH;
    }

    @MustBeClosed
    public Scope openScope() {
      writeLine("{");
      return new Scope(this);
    }

    @MustBeClosed
    public Scope openScope(String expr) {
      if (expr.isEmpty()) {
        throw new IllegalArgumentException("Open scope expression may not be empty");
      }
      writeLine(expr + " {");
      return new Scope(this);
    }

    @FormatMethod
    @MustBeClosed
    public Scope openScope(String format, Object... args) {
      return openScope(String.format(format, args));
    }

    public void writeLine(String line) {
      // If there is an open child scope then this is a programming error.
      checkState(!this.hasOpenScope);
      for (int i = 0; i < this.indent; i++) {
        this.root.buf.append(' ');
      }
      this.root.buf.append(line);
      this.root.buf.append(System.lineSeparator());
    }

    @FormatMethod
    public void writeLine(String format, Object... args) {
      writeLine(String.format(format, args));
    }

    @Override
    public void close() {
      // You should not close the root scope.
      checkState(this.parent.isPresent());
      var parent = this.parent.get();
      parent.hasOpenScope = false;
      parent.writeLine("}");
    }
  }

  @Override
  public String toString() {
    return this.buf.toString();
  }

  public CodeGeneratorResponse.File buildResponse(Descriptor messageDescriptor, String scope) {
    checkState(!this.rootScope.hasOpenScope);
    return CodeGeneratorUtils.initializeFileBuilder(messageDescriptor, scope)
        .setContent(this.buf.toString())
        .build();
  }

  public CodeGeneratorResponse.File buildResponse(EnumDescriptor enumDescriptor) {
    checkState(!this.rootScope.hasOpenScope);
    return CodeGeneratorUtils.initializeFileBuilder(enumDescriptor)
        .setContent(this.buf.toString())
        .build();
  }
}
