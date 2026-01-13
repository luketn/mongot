package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.TimestampProvider;
import com.xgen.mongot.util.bson.ByteUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.bson.BsonBinaryReader;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonSerializationException;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a directory and a set of FTDC archive and interim files. The archive files are a sequence
 * of Bson documents. Whereas the interim file has only the one document written at a time.
 */
class FtdcFileManager {
  /**
   * FTDC files are named with a timestamp and a 5 digit incrementing id (for instance:
   * metrics.2020-09-26T17-48-31Z-00000). We increment this id in case there is more than one file
   * written per second. It is very unlikely that this will happen more than once, in a sane
   * configuration FTDC flushes every few minutes and rotates files even less frequently.
   *
   * <p>This constant is setting an upper bound for searching a new file name.
   */
  private static final int MAX_FILES_PER_SECOND = 10000;

  private static final Logger LOG = LoggerFactory.getLogger(FtdcFileManager.class);
  private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = BsonUtils.BSON_DOCUMENT_CODEC;

  private final FtdcConfig config;
  private final Path dir;

  /** Path to a file used to write one bson document we would want to recover in case of a crash. */
  private final Path interimFile;

  private final FtdcMetadata metadata;

  private Path currentFile;

  private final TimestampProvider timestampProvider = TimestampProvider.terseTimeStamp();

  private FtdcFileManager(FtdcConfig config, FtdcMetadata metadata) throws IOException {
    this.config = config;
    this.dir = this.config.ftdcDirectory;
    this.interimFile = this.dir.resolve("metrics.interim");
    this.metadata = metadata;
    this.currentFile = newArchiveFile(metadata);
  }

  static FtdcFileManager initialize(FtdcConfig config, FtdcMetadata metadata) throws IOException {
    FileUtils.mkdirIfNotExist(config.ftdcDirectory);

    FtdcFileManager manager = new FtdcFileManager(config, metadata);
    manager.recoverInterimFile();
    manager.removeOldFilesIfLimitsExceeded();
    return manager;
  }

  /**
   * Atomically replaces (or writes if the file does not exist) the interim file with this document.
   */
  void replaceInterim(BsonDocument document) throws IOException {
    var bytes = ByteUtils.toByteArray(document);
    FileUtils.atomicallyReplace(this.interimFile, bytes);
  }

  /**
   * Writes document to an archive file, deletes the interim file if it exists. Performs
   * housecleaning tasks rotation and garbage collection.
   */
  void writeAndClearInterim(BsonDocument document) throws IOException {
    var bytes = ByteUtils.toByteArray(document);

    // note that writes to this.currentFile are not atomic. If we crash during this write,
    // incomplete data will be written. It is acceptable to have a truncated last record and it will
    // be ignored by t2. This is the reason why we clean the interim file only after this write
    // finished.
    Files.write(
        this.currentFile,
        bytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND,
        StandardOpenOption.SYNC);

    deleteInterimIfExists();
    rotateIfNeeded();
    removeOldFilesIfLimitsExceeded();
  }

  private void rotateIfNeeded() throws IOException {
    checkState(this.currentFile.toFile().exists(), "tried to rotate file that does not exist");
    long currentFileSize = this.currentFile.toFile().length();
    if (currentFileSize > this.config.archiveFileSize.toBytes()) {
      rotate();
    }
  }

  private void removeOldFilesIfLimitsExceeded() throws IOException {
    Predicate<File> notCurrentFile = file -> !file.equals(this.currentFile.toFile());
    LinkedList<File> files =
        Arrays.stream(Optional.ofNullable(this.dir.toFile().listFiles()).orElse(new File[0]))
            .filter(notCurrentFile)
            // reversed order has older files last.
            .sorted(Comparator.comparing(File::lastModified).reversed())
            .collect(Collectors.toCollection(LinkedList::new));

    @Var Bytes currentDirSize = Bytes.ofBytes(files.stream().mapToLong(File::length).sum());

    // remove files until we are under the limit
    while (!files.isEmpty()
        && (currentDirSize.compareTo(this.config.directorySize) > 0
            || files.size() > this.config.maxFileCount)) {
      File oldest = files.removeLast();
      currentDirSize = Bytes.ofBytes(currentDirSize.toBytes() - oldest.length());
      Files.delete(oldest.toPath());
    }
  }

  private void recoverInterimFile() throws IOException {
    Optional<BsonDocument> doc = tryReadDocumentFromInterim();
    if (doc.isPresent()) {
      writeAndClearInterim(doc.get());
    }

    deleteInterimIfExists();
  }

  private Optional<BsonDocument> tryReadDocumentFromInterim() throws IOException {
    if (!this.interimFile.toFile().exists()) {
      return Optional.empty();
    }

    LOG.info("found interim file, recovering");
    byte[] data = Files.readAllBytes(this.interimFile);
    ByteBuffer buffer = ByteBuffer.wrap(data);
    return tryReadDocument(buffer);
  }

  private static Optional<BsonDocument> tryReadDocument(ByteBuffer buffer) {
    try {
      BsonDocument document =
          BSON_DOCUMENT_CODEC.decode(
              new BsonBinaryReader(buffer), DecoderContext.builder().build());
      return Optional.of(document);

    } catch (BsonSerializationException e) {
      LOG.atInfo()
          .addKeyValue("exceptionMessage", e.getMessage())
          .log("failed reading interim file, possibly corrupt");
      return Optional.empty();
    }
  }

  private void deleteInterimIfExists() throws IOException {
    Files.deleteIfExists(this.interimFile);
  }

  void rotate() throws IOException {
    this.currentFile = newArchiveFile(this.metadata);
  }

  private Path newArchiveFile(FtdcMetadata metadataDoc) throws IOException {
    Path file = uniqueFileName();

    BsonDocument metadata =
        new BsonDocument()
            .append("_id", new BsonDateTime(new Date().getTime()))
            .append("type", DocumentType.METADATA_TYPE)
            .append("doc", metadataDoc.serialize());
    var bytes = ByteUtils.toByteArray(metadata);

    Files.write(file, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC);

    return file;
  }

  private Path uniqueFileName() throws IOException {
    String timestamp = this.timestampProvider.timestamp();

    for (int i = 0; i < MAX_FILES_PER_SECOND; i++) {
      String name = String.format("metrics.%s-%05d", timestamp, i);
      Path path = this.dir.resolve(name);
      if (!path.toFile().exists()) {
        return path;
      }
    }

    throw new IOException(
        String.format("maximum ftdc files written for this second %s", timestamp));
  }
}
