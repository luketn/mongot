load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "testing",
    testonly = True,
    srcs = [
        "BsonDeserializationTestSuite.java",
        "BsonSerializationTestSuite.java",
        "BsonTestSuite.java",
        "BsonTestUtils.java",
        "ConcurrencyTestUtils.java",
        "ControlledBlockingQueue.java",
        "ControlledPriorityBlockingQueue.java",
        "JsonTestSuite.java",
        "LuceneIndexRule.java",
        "LuceneTestUtils.java",
        "TestUtils.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_flogger_flogger",  # keep: flogger for libs under test
        "@maven//:com_google_flogger_flogger_slf4j_backend",  # keep: flogger for libs under test
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/codec",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:ch_qos_logback_logback_core",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
        "@maven//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",  # keep: flogger for libs under test
        "@maven//:com_google_flogger_flogger_slf4j_backend",  # keep: flogger for libs under test
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
