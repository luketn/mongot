# Checkstyle tests in Mongot

The file `mongot.xml` configures the checkstyle lint tests. It is copied
from [google_checks.xml](https://github.com/checkstyle/checkstyle/blob/master/src/main/resources/google_checks.xml)
with some custom rules for mongot.

## Updating checkstyle

The checkstyle binary, intellij plugin, google-java-formatter, and `mongot.xml` all work together to
enforce the Google Java Style Guide. These can all be updated independently to some degree, but work
best when kept in-sync.

1. Update the google-java-format Intellij plugin using `Intellij IDEA > settings > plugins`
2. Update checkstyle binary to the latest version in `//java/deps.bzl`
   1. Update Intellij checkstyle plugin to the latest version
   2. Select the latest checkstyle version
      in `Intellij IDEA > settings > tools > checkstyle > checkstyle version`
3. Copy the latest version
   of [google_checks.xml](https://github.com/checkstyle/checkstyle/blob/master/src/main/resources/google_checks.xml)
   into [mongot.xml](./mongot.xml)
   1. Ensure customized checks at the bottom are preserved

## Resolving conflicts with google-java-format

`google_checks.xml` attempts to enforce
the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html), but is not
maintained by Google. It's also not
guaranteed to be consistent with the `google-java-format` binary. When checkstyle
and `google_java_format` disagree, we should use the following steps to ensure
our checkstyle config accepts the results of the automatic formatting:

1. Follow steps in [Updating Checkstyle](#Updating-checkstyle) to update checkstyle and
   google-java-format.
2. If the problem persists, add a `SuppressionXpathSingleFilter` to ./mongot.xml to disable the lint
   check in as narrow a context as possible. (see block `Begin custom overrides for Mongot`)