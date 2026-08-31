# Discarded attempts

Nothing in this directory contributes to any number reported in
[EXPERIMENTS.md](../../../EXPERIMENTS.md). It is kept because a discarded attempt
is evidence about the harness, and deleting it would leave only the runs that
happened to work.

| Directory | What it was | Why it is not used |
| --- | --- | --- |
| `preflight-abort-arm-A/` | Arm A, first attempt | Aborted in preflight on trial-001: the pod never became Ready. The orchestrator's `--image` default tracked the build version, which had just become `git describe` output, so it asked for an image tag that only existed locally under a different name and the kubelet tried to pull it from Docker Hub. No trial produced a measurement. |
| `matrix-attempt-1/` | All five arms, 25 complete trials | Every trial succeeded and every arm was internally consistent, but the run was not produced by a single build: arm A came from commit `70142e0` and arms B–E from `c7b7e94`, with some probe images stamped `-dirty`. The diff between those commits touches only post-hoc aggregation code, so the measurements are expected to agree with the rerun — but that is an argument from a diff rather than a property of the record. |

Each directory has its own `REASON.txt` with the full diagnosis and the code fix
that followed. Both defects now fail loudly rather than being caught by hand:
the image tag is pinned apart from the build version, and the aggregator treats
a build difference between repeats as a disagreement.
