#!/bin/bash
# Show MongoDB's edits to these 2 files that were temporarily copied from Lucene as a workaround.
if [ -z "$1" ]; then
  echo "$0 [lucene 9.11.1 path]"
else
  diff -u $1/lucene/core/src/java/org/apache/lucene/codecs/lucene99/OffHeapQuantizedByteVectorValues.java OffHeapQuantizedByteVectorValues.java
  diff -u $1/lucene/codecs/src/java/org/apache/lucene/codecs/bitvectors/FlatBitVectorsScorer.java BinaryQuantizedFlatVectorsScorer.java

  wc -l OffHeapQuantizedByteVectorValues.java BinaryQuantizedFlatVectorsScorer.java
fi
