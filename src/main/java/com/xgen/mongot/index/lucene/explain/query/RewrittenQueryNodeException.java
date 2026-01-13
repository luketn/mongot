package com.xgen.mongot.index.lucene.explain.query;

import java.util.List;
import java.util.Optional;

public class RewrittenQueryNodeException extends Exception {

  RewrittenQueryNodeException(String message) {
    super(message);
  }

  RewrittenQueryNodeException(String message, Throwable cause) {
    super(message, cause);
  }

  public static RewrittenQueryNodeException wrapWithRootsAndRethrow(
      List<? extends QueryExecutionContextNode> rootNodes, RewrittenQueryNodeException e) {

    StringBuilder message = new StringBuilder("Root Nodes:\n");

    for (QueryExecutionContextNode node : rootNodes) {
      message.append(node.toString()).append("\n");
    }

    message.append("\nOriginal Exception:\n").append(e.getMessage());

    return new RewrittenQueryNodeException(message.toString(), e);
  }

  public static void checkNodesAreEqual(
      RewrittenQueryExecutionContextNode first, RewrittenQueryExecutionContextNode second)
      throws RewrittenQueryNodeException {
    if (first.equals(second)) {
      return;
    }

    var errorMessage =
        messageWithNodes(
            "Query tree mismatch: The structures of the provided nodes are not identical",
            Optional.of(first),
            Optional.of(second));

    throw new RewrittenQueryNodeException(errorMessage);
  }

  public static String messageWithNodes(
      String errorMessage,
      Optional<RewrittenQueryExecutionContextNode> first,
      Optional<RewrittenQueryExecutionContextNode> second) {
    return String.format(
        "%s: %n%n" + "First Tree Structure:%n%s%n%n" + "Second Tree Structure:%n%s%n",
        errorMessage,
        first.isPresent() ? first.get() : first,
        second.isPresent() ? second.get() : second);
  }

  public static String messageWithClauses(
      String errorMessage,
      Optional<RewrittenChildClauses> first,
      Optional<RewrittenChildClauses> second) {
    return String.format(
        "%s: %n%n" + "First Clause Structure:%n%s%n%n" + "Second Clause Structure:%n%s%n",
        errorMessage,
        first.isPresent() ? first.get() : first,
        second.isPresent() ? second.get() : second);
  }
}
