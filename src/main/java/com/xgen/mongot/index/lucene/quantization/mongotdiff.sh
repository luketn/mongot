#!/bin/bash
# Show MongoDB's edits to these 3 files that were permanently copied from Lucene as templates.
if [ -z "$1" ]; then
  echo "$0 [lucene 9.11.1 path]"
else
  diff -u $1/lucene/core/src/java/org/apache/lucene/codecs/lucene99/Lucene99ScalarQuantizedVectorsFormat.java Mongot01042BinaryQuantizedFlatVectorsFormat.java
  diff -u $1/lucene/core/src/java/org/apache/lucene/codecs/lucene99/Lucene99ScalarQuantizedVectorsReader.java Mongot01042BinaryQuantizedFlatVectorsReader.java
  diff -u $1/lucene/core/src/java/org/apache/lucene/codecs/lucene99/Lucene99ScalarQuantizedVectorsWriter.java Mongot01042BinaryQuantizedFlatVectorsWriter.java

  wc -l Mongot01042BinaryQuantizedFlatVectorsFormat.java Mongot01042BinaryQuantizedFlatVectorsReader.java Mongot01042BinaryQuantizedFlatVectorsWriter.java
fi
