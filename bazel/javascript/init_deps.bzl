load("@npm_search_index_schema//:repositories.bzl", "npm_repositories")

def javascript_init_deps():
    # Initialize npm repositories for search-index-schema
    npm_repositories()
