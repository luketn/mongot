# MongoT Trace Scenario Breakdown Summary

Generated: 2026-04-26 23:24:10.

| Scenario | Report | Chart | Requests | Throughput | HTTP median | MongoT stream median | MongoT command median | Trace hits |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Text Search Only | [trace-breakdown-text-full-20260426-225217-01-text.md](./trace-breakdown-text-full-20260426-225217-01-text.md) | [trace-breakdown-text-full-20260426-225217-01-text.png](./trace-breakdown-text-full-20260426-225217-01-text.png) | 166,253 | 554.070121 req/s | 39.186 ms | 8.913 ms | 890 us | 2,000 |
| Vector Search Only | [trace-breakdown-vector-full-20260426-225217-02-vector.md](./trace-breakdown-vector-full-20260426-225217-02-vector.md) | [trace-breakdown-vector-full-20260426-225217-02-vector.png](./trace-breakdown-vector-full-20260426-225217-02-vector.png) | 56,594 | 188.589251 req/s | 124.21 ms | 9.979 ms | 2.797 ms | 2,000 |
| Both Vector And Text | [trace-breakdown-both-full-20260426-225217-03-both.md](./trace-breakdown-both-full-20260426-225217-03-both.md) | [trace-breakdown-both-full-20260426-225217-03-both.png](./trace-breakdown-both-full-20260426-225217-03-both.png) | 23,388 | 77.806564 req/s | 268.458 ms | 37.824 ms | 2.175 ms | 2,000 |
| Text Search With License Fields | [trace-breakdown-text-license-full-20260426-225217-04-text-license.md](./trace-breakdown-text-license-full-20260426-225217-04-text-license.md) | [trace-breakdown-text-license-full-20260426-225217-04-text-license.png](./trace-breakdown-text-license-full-20260426-225217-04-text-license.png) | 67,688 | 225.452906 req/s | 86.988 ms | 11.901 ms | 863 us | 2,000 |
| Random Synthetic Deletes And Inserts | [trace-breakdown-mutations-mutation-20260426-231908-01-mutations.md](./trace-breakdown-mutations-mutation-20260426-231908-01-mutations.md) | [trace-breakdown-mutations-mutation-20260426-231908-01-mutations.png](./trace-breakdown-mutations-mutation-20260426-231908-01-mutations.png) | 11,324 | 37.74378 req/s | 5.413 ms | n/a | n/a | 20,444 |

Mutation note: a 25-VU mutation run was attempted first and failed k6 thresholds because the Coco `/image/add` API allocates ids with `nextImageId()` before insertion, which races under concurrent inserts. The final mutation breakdown uses one VU for a clean API-driven insert/delete trace run.
