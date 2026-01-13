load(
    "@com_github_bazelbuild_buildtools//buildifier:deps.bzl",
    _buildifier_dependencies = "buildifier_dependencies",
)

def buildifier_deps():
    _buildifier_dependencies()
