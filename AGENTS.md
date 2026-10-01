# AGENTS.md

Instructions for any coding agent (and human) working in this repository.

## Project

Rust port of `../wt-tracker`, a WebTorrent tracker in Node.js + uWebSockets.js. The goals are
multi-core operation, zero-copy message handling and fast WebSockets.

- `crates/wt-core`: sans-IO tracker math (`Shard`): peers, swarms, offer routing.
- `crates/wt-bench`: Rust benchmarks. `bench/js`: identical JS benchmarks against `../wt-tracker`.
- `docs/SPEC.md`: **the specification and source of truth for behaviour.**
- `.agents/skills/`: shared agent skills (`SKILL.md` format). `.claude/skills` is a symlink to
  it for Claude Code; keep skills in `.agents/skills/` only.

## Specification rules

Code and spec change together. A change is not finished while `docs/SPEC.md` describes the old
behaviour.

1. **Before changing code**, read `docs/SPEC.md`: at least §3–§6, plus the sections your change
   touches. If the requested change contradicts the spec, say so and confirm the new behaviour
   before implementing it.
2. **With every code change**, update `docs/SPEC.md` in the same change:

   | You changed | Update |
   |---|---|
   | request handling, offer selection, removal/expiry rules, outbox events | §5 |
   | `Settings` fields or defaults | §6 |
   | data structures, `Key`, indices | §3, and §4 plus `check_invariants()` if invariants change |
   | behaviour that now differs from (or matches) the JS `FastTracker` | §8 |
   | test layout or requirements | §9 |
   | benchmark scenarios (always add the Rust **and** the JS twin) | §10 |
   | crates / directories / architecture | §2 |
   | something done, or newly planned | §12 (planned work goes only here) |

3. Describe the implemented behaviour precisely: names, defaults, edge cases, error variants. Put
   no aspirations outside §12. Keep the section numbering; extend tables rather than adding
   prose.
4. **Never edit §11 (performance) by hand.** It is generated between the `perf-tables` markers.

## Performance tables

When a change can affect performance (anything in `crates/wt-core/src`, `crates/wt-bench`,
`bench/js`), regenerate §11:

```bash
./bench/run.sh
```

It takes about 3 minutes and needs `../wt-tracker` with its `node_modules` (or set
`WT_TRACKER_DIR`). Run it on an idle machine, because background load skews the results. The
"Same messages" column must be `yes` for every twin scenario; `no` means Rust and JS diverged.
Report significant regressions or improvements.

## Finish checklist

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
node difftest/run.ts          # JS vs Rust differential test (needs ../wt-tracker)
./scripts/check-spec.sh
```

`check-spec.sh` fails when code changed but `docs/SPEC.md` did not. If a change truly needs no
spec update (a pure refactor, a comment, a dependency bump), rerun it with
`SPEC_UNCHANGED_OK=1` and say why.
