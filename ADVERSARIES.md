# Orca-B scheduling adversaries

From the controller's `benchmark/` directory, after choosing the desired existing deployment branch/commit in the normal repository workflow:

The controller's committed `HEAD` must be fetchable by the AWS machines from the deployment repository and branch configured in `settings.json`; existing remote clones must have the corresponding `origin`. This delivery provides local feature branches and patches/Git bundles, and does not push the commits to a remote repository. Publish the chosen commit to the configured deployment branch before running the AWS workflow, or apply the patch there, commit it, and check out that published commit on the controller. A branch or bundle present only on the local machine is not automatically available to the AWS machines.

```bash
fab create --nodes 4
fab install
fab remote --nodes 4 --workers 1 --rate 10000 --tx-size 512 --duration 60 --faults 0 --runs 1 --adversary none
fab remote --nodes 4 --workers 1 --rate 10000 --tx-size 512 --duration 60 --faults 0 --runs 1 --adversary silent --byzantine 1 --seed 7 --start-round 5 --attack-rounds 40
fab remote --nodes 4 --rate 10000 --duration 60 --faults 0 --runs 1 --adversary targeted --byzantine-ids 1 --seed 7 --delay-ms 200
fab remote --nodes 4 --rate 10000 --duration 60 --faults 0 --runs 1 --adversary network --byzantine 0 --seed 7 --delay-ms 200
```

`create --nodes 4` requests four machines in each configured region; `remote --nodes 4` selects a four-authority committee from the existing testbed. Only create the machines required by your chosen regional layout.

Without `--adversary`, Orca-B preserves `nodes=20`, `workers=1`, `rate=50000`, `tx_size=512`, `duration=80`, `faults=0`, and `runs=1`.

Orca's legacy `faults` controls a moving proposer schedule. Explicit modes require `--faults 0` and use the separate fixed Byzantine options. Their primary commands force `ORCA_FAULTS=0`; the old client silence schedule is not generated. This also applies to explicit `none/A0`. A missing adversary option keeps legacy behavior.

Orca has an ordinary leader target each round. `silent` and `targeted` require a nonempty fixed Byzantine set; `network` can run with an empty set. The Bullshark alternation option is accepted for a common command interface but does not change Orca's schedule.

The window is keyed to a proposal/vote's evidence round, or to the leader-round instance for ABA. New-round proposal and send behavior resumes outside the window. A proposal withheld during a silent round is not created retroactively, and repair traffic for evidence inside that round window can remain omitted; the window does not promise later replay of all previously withheld evidence. Explicit modes do not use the legacy worker `BatchSilent` control or client silence schedule.

The existing controller workflow remains `fab create`, `fab install`, then `fab remote`. Run these commands from `benchmark/` with your existing `settings.json`. For Orca-B, `fab install` prepares dependencies and clones the repository when needed. Each `fab remote` fetches the configured branch, checks out the controller's exact commit on the selected AWS machines, and builds there; it also builds the controller binaries before generating the run configuration. Changing only adversary parameters requires another `fab remote`, which still follows this existing build path. Use `fab --help remote` to inspect the parameters.

A mode is opt-in. Omitting `--adversary` keeps the old run defaults and legacy fault behavior. Explicit `--adversary none` (alias `A0`) is the no-injection baseline. The only aliases are:

| Mode | Alias | Behavior |
| --- | --- | --- |
| `none` | `A0` | No scheduling injection. |
| `silent` | `A1` | Fixed Byzantine authorities suppress proposals and selected primary protocol sends during the attack window. |
| `targeted` | `A3` | Fixed Byzantine senders use a deterministic, leader-dependent recipient schedule to delay valid signed evidence. |
| `network` | `A5` | The delivery schedule can also delay sends from honest authorities. An empty fixed Byzantine set is allowed. |

`targeted` is selective scheduling of existing valid messages. It does not generate conflicting signed proposals or implement an equivocation/fork attack. A2 and A4 are not implemented aliases. Scheduling changes delivery and proposal emission; signatures, certificate checks, quorum thresholds, and consensus commit rules keep their original meaning.

The common experiment flags are `--nodes`, `--workers`, `--rate`, `--tx-size`, `--duration`, `--faults`, and `--runs`. `--nodes` takes one count. `--rate 10000,50000` runs those input rates in order. All new mode commands default to `faults=0` if the option is omitted.

| Adversary option | Default | Meaning |
| --- | --- | --- |
| `--byzantine` | `0` | Fixed Byzantine count; selects launch IDs `0..count-1`. |
| `--byzantine-ids` | empty | Explicit comma-separated launch IDs, e.g. `0,2`; use instead of a positive count. |
| `--seed` | `1` | Deterministic scheduling seed, unsigned 64-bit integer. |
| `--start-round` | `1` | First protocol round affected; at least 1. |
| `--attack-rounds` | `0` | Window length; zero is unbounded. |
| `--delay-ms` | `200` | Delay of selected sends, from 1 through 86,400,000 ms. |
| `--max-pending-messages` | `4096` | Positive per-primary bound on admitted delayed messages, including sends still awaiting transport completion. |
| `--max-pending-bytes` | `67108864` | Positive per-primary bound on their retained serialized message bytes. |
| `--bullshark-alternating` | `true` | Bullshark only; accepts the value `true` or `false`. |

The bounded window is `[start_round, start_round + attack_rounds)`; overflow is rejected. Queue bounds fit the 64-bit deployment target. Fixed Byzantine membership is selected once per committee: its size cannot exceed `f = floor((nodes - 1) / 3)`, IDs must be unique and in range, and they refer to key-generation/launch order (the ID used by `.node-<id>.json`), not a public-key sort order or a shortened display ID. Each primary receives the corresponding full public keys. Parameter errors are rejected before constructing the AWS manager.

Every explicit-mode run writes a timestamped `results/*-n<N>-r<R>-run<I>.parameters.json` file before launch, including benchmark parameters, node parameters, the full launch-ID-to-public-key array, and resolved runtime configuration. The controller prints its path. Keep this file with the run's logs and normal performance results. A seed is deterministic for the same committee, parameters, and protocol rounds; `fab remote` generates new keys, so the seed alone is not a promise of the same identity-dependent schedule across fresh committees.

The primary environment contains one shell-quoted `BENCH_ADVERSARY_CONFIG` JSON object with `mode`, `byzantine_authorities`, `seed`, `start_round`, `rounds`, `delay_ms`, `max_pending_messages`, `max_pending_bytes`, and `bullshark_alternating`. Fabric resolves aliases and launch IDs before passing it to Rust. The benchmark clients and workers continue to receive ordinary workload commands.

An admitted delayed send remains counted until transport completion/acknowledgement. Its timer does not require new incoming messages, and later consensus garbage collection cannot discard it before release. When the scheduling budget is full, or a message exceeds its byte limit, that message goes immediately through the ordinary transport. The limits bound scheduling storage, not total node memory: the original transport and consensus buffers still exist, and a silent proposer may retain incoming worker batch digests until it can propose again.

The runtime strategy is centralized in `primary/src/adversary.rs`. Python's `benchmark/benchmark/adversary.py` only validates experiment parameters, maps launch IDs to keys, and serializes the configuration. Supporting transport timers and queue accounting implement delivery mechanics; they do not choose adversary strategies.

Offline Python validation, from `benchmark/` after installing the repository's normal Python requirements:

```bash
python3 -m unittest discover -s tests -v
```

The new tests run the real Fabric argument parser, validate the config and ID mapping, capture real primary/client launch commands, and execute the nested shell command through Bash with a fake tmux/node boundary. They do not contact AWS.
