load("@aspect_rules_js//npm:repositories.bzl", "npm_translate_lock")

def javascript_deps():
    # Translate the package-lock.json for search-index-schema to Bazel targets
    # Note: aspect_rules_js converts npm's package-lock.json to pnpm-lock.yaml internally
    npm_translate_lock(
        name = "npm_search_index_schema",
        pnpm_lock = "//packages/search-index-schema:pnpm-lock.yaml",
        npm_package_lock = "//packages/search-index-schema:package-lock.json",
        verify_node_modules_ignored = "//:.bazelignore",
        data = [
            "//packages/search-index-schema:package.json",
        ],
    )
