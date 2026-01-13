load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

mongot_java_unit_test_suite(
    name = "lib",
    timeouts = {
        "TestLuceneStoredFieldsFormat.java": "moderate",
    },
    deps = [
        "org.apache.lucene:lucene-core",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/codec",
        "//src/main/java/com/xgen/mongot/index/lucene/codec/flat",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/quantization",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_flogger_flogger_system_backend",
        "@maven//:org_apache_lucene_lucene_test_framework",
    ],
)
