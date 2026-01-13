load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "serialization",
    testonly = True,
    srcs = [
        "BsonProcessorTestSpec.java",
        "IndexingTestSuite.java",
        "LuceneIndexedBinaryField.java",
        "LuceneIndexedFieldSpec.java",
        "LuceneIndexedGeoShapeField.java",
        "LuceneIndexedKnnVectorField.java",
        "LuceneIndexedLatLonPointField.java",
        "LuceneIndexedNumericField.java",
        "LuceneIndexedStoredSourceField.java",
        "LuceneIndexedStringField.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/testing/mongot/integration/index/serialization",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/extension",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing/mongot/integration/index/serialization",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
