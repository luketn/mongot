/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

// This file is copied from Lucene 9.11.1 branch.
// https://github.com/apache/lucene/blob/releases/lucene/9.11.1/lucene/codecs/src/java/org/apache/lucene/codecs/bitvectors/FlatBitVectorsScorer.java
// There are no modifications except a different package name.

/** A bit vector scorer for scoring byte vectors. */
public class FlatBitVectorsScorer implements FlatVectorsScorer {
  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (vectorValues instanceof RandomAccessVectorValues.Bytes) {
      return new BitRandomVectorScorerSupplier((RandomAccessVectorValues.Bytes) vectorValues);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    throw new IllegalArgumentException("bit vectors do not support float[] targets");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (vectorValues instanceof RandomAccessVectorValues.Bytes) {
      return new BitRandomVectorScorer((RandomAccessVectorValues.Bytes) vectorValues, target);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Bytes");
  }

  @Override
  public String toString() {
    return "FlatBitVectorsScorer()";
  }
}
