load("@rules_jvm_external//:specs.bzl", "maven", "parse")

def append_version(version, artifacts):
    """
    Converts a list of partial maven coordinates (group and artifact id) to full
    maven coordinates by appending the version to each artifact. Useful for ensuring consistent
    versions across multiple artifacts.

    e.g. append_version("4.13.2", ["junit:junit"]) yields ["junit:junit:4.13.2"]
    """
    appended = []
    for artifact in artifacts:
        appended.append("{}:{}".format(artifact, version))

    return appended

def as_test_only(mvn_coords):
    """
    Converts a list of Maven coordinate strings to a list of testonly maven_artifact targets. Test
    only libraries cannot be transitively depended on by production code.

    input should contain a group, artifact id, and version separated by a colon, e.g.
    [
        "junit:junit:4.13.2",
        "com.google.truth:truth:1.1.3",
    ]
    """
    return [
        maven.artifact(
            group = dep["group"],
            artifact = dep["artifact"],
            version = dep["version"],
            testonly = True,
        )
        for dep in parse.parse_artifact_spec_list(mvn_coords)
    ]

def as_neverlink(mvn_coords):
    """
    Converts a list of Maven coordinate strings to a list of neverlink maven_artifact targets.
    Neverlink are similar to "provided" dependencies in Maven. They are available at compile time but not
    included in the runtime classpath.


    input should contain a group, artifact id, and version separated by a colon, e.g.
    [
        "junit:junit:4.13.2",
        "com.google.truth:truth:1.1.3",
    ]
    """
    return [
        maven.artifact(
            group = dep["group"],
            artifact = dep["artifact"],
            version = dep["version"],
            neverlink = True,
        )
        for dep in parse.parse_artifact_spec_list(mvn_coords)
    ]
