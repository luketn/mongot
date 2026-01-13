load("//bazel/java:dep_utils.bzl", "append_version", "as_test_only")

_LUCENE_VERSION = "9.11.1"
_LUCENE_ARTIFACTS = append_version(
    _LUCENE_VERSION,
    [
        "org.apache.lucene:lucene-analysis-common",
        "org.apache.lucene:lucene-analysis-icu",
        "org.apache.lucene:lucene-analysis-kuromoji",
        "org.apache.lucene:lucene-analysis-morfologik",
        "org.apache.lucene:lucene-analysis-nori",
        "org.apache.lucene:lucene-analysis-phonetic",
        "org.apache.lucene:lucene-analysis-smartcn",
        "org.apache.lucene:lucene-analysis-stempel",
        "org.apache.lucene:lucene-backward-codecs",
        "org.apache.lucene:lucene-core",
        "org.apache.lucene:lucene-expressions",
        "org.apache.lucene:lucene-highlighter",
        "org.apache.lucene:lucene-join",
        "org.apache.lucene:lucene-misc",
        "org.apache.lucene:lucene-queries",
        "org.apache.lucene:lucene-queryparser",
        "org.apache.lucene:lucene-facet",
    ],
) + as_test_only(append_version(_LUCENE_VERSION, ["org.apache.lucene:lucene-test-framework"]))

SEARCH_QUERY_DEPS = _LUCENE_ARTIFACTS
