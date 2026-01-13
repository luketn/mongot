# FTDC
Stores time series metrics in FTDC format.

## See Also:

- [FTDC specification](https://docs.google.com/document/d/1CRUnSIs8C-giQ-eyT1CGcuViFVxiKHjPpku1z1HWyEk/edit#)
- [t2](https://github.com/10gen/t2): visualizations for FTDC

## Glossary

- Meter: is some number that changes with time, for instance "memory", or "number of indexes". See [Micrometer](https://micrometer.io/docs) docs for what meter types are supported. 
- Sample: meter value at a given point in time. FTDC samples all the collected meters at given intervals.

## Entry Points
[Ftdc](Ftdc.java) is the entry point of the library.
See also [FtdcConfig](FtdcConfig.java) for the available configuration parameters of FTDC.

## Using locally

If you want to examine ftdc metrics for a locally running mongot, run this:
```commandline
$ make docker.ftdc TARGET_MONGOT=1
Metrics exported to /tmp/mongot.tLMlAY/diagnostic.data
You can now run:
   t2 /tmp/mongot.tLMlAY/diagnostic.data
```