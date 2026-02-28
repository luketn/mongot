## IntelliJ QuickStart 

Run the following to start mongod locally and configure mongot to talk to it:
```bash
./run-local.sh
```

Configure IntelliJ run config:  

1. Add a Bazel target
Set the target to run to:
```
@//:mongot_community__non_stamped
```
2. Save the Run config, clicking OK
3. Open the Run config settings, and add the Program arguments:
```
--config /Users/luketn/code/personal/mongot/mongot-dev.yml --internalListAllIndexesForTesting
```
![intellij-run-config.png](intellij-run-config.png)
