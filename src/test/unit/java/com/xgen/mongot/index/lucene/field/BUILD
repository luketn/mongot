load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "field",
    srcs = ["FieldNameTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)
