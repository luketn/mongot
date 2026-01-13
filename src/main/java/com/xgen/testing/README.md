# Testing

This subtree contains classes that aid writing unit and integration tests but do not themselves
contain test targets.


Test helper methods and classes used within a single package may reside in `src/test`, but 
code reused more widely should be placed here in and ideally have corresponding unit tests in
`src/test/unit`.

Rules in this tree should be declared with `testonly = True,`