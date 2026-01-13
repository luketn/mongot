package com.xgen.mongot.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Responsible for mutating the state of an Index, either in response to a DocumentEvent or because
 * of an administrative request such as dropping all indexed Documents.
 *
 * <p>Prior to calling close() on an IndexWriter, all other Objects using the IndexWriter must cease
 * using all its methods.
 */
public interface IndexWriter extends Closeable {

  /** Updates the index to reflect the changed document. */
  void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException;

  /**
   * Commits the index, along with the supplied user data that can later be retrieved via
   * getCommitUserData.
   */
  void commit(EncodedUserData userData) throws IOException;

  /**
   * Returns the last set of user data that was committed, or an empty map if the index has not been
   * committed yet.
   */
  EncodedUserData getCommitUserData();

  /**
   * Whether or not this index has exceeded usage limits.
   *
   * <p>There two limits:
   *
   * <ul>
   *   <li>Number of fields in a single document. See {@code FieldExceededLimitsException}.
   *   <li>Number of documents in a single index. See {@code DocsExceededLimitsException}.
   * </ul>
   */
  Optional<ExceededLimitsException> exceededLimits();

  /** Returns the number of unique fields indexed by the IndexWriter. */
  int getNumFields() throws WriterClosedException;

  /** Removes all indexed documents from the collection and commits it with the given userData. */
  void deleteAll(EncodedUserData userData) throws IOException;

  @Override
  void close() throws IOException;

  /**
   * The total number of docs in this index. Depending on implementation, the returned value may be
   * approximate.
   */
  long getNumDocs() throws WriterClosedException;
}
