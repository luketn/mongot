load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "ftdc",
    srcs = [
        "FtdcCollectorTest.java",
        "FtdcCompressorTest.java",
        "FtdcFileManagerTest.java",
        "FtdcMetadataTest.java",
        "FtdcScheduledReporterTest.java",
        "FtdcTest.java",
        "FtdcTestUtil.java",
        "LongPackerTest.java",
        "ZlibUtilTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/metrics/ftdc",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
