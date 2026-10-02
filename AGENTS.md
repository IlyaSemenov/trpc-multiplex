# trpc-multiplex Agent Guide

## Overview

Multiplex multiple tRPC subscriptions over a single HTTP stream.

Read [README.md](README.md) completely before changing the public API, package behavior, supported runtimes, or user documentation.

Extend this guide only with stable, non-obvious conventions, architecture, contracts, workflows, and gotchas.
Do not catalog files or restate information evident from their names and locations.

## Scope

- Keep production code in `src/`.
- Test behavior through the public entry points whenever possible: keep such integration, package-boundary, and type-inference tests in `tests/`, importing the package by name as mapped by `tsconfig.json` paths.
- Keep tests that need internals of one module, such as the wire protocol of the server, beside their source as `*.test.ts`.
- Keep tests that span several modules and use internals in `src/__tests__/` with relative imports.
- Reuse the test server and router of `tests/harness.ts` in tests under `src/` instead of duplicating them.
- Name compile-only tests `*.type-test.ts`.
- Keep `src/client/index.ts`, `src/server/index.ts`, and `src/worker/index.ts` limited to explicit public exports.
- Keep browser tests in `e2e/` as `*.e2e.ts`; they run the built package in a Vite app, in both the dev server and the production build.
- Treat `package.json` exports and supported runtimes as public contracts.

## Architecture

- Keep the multiplexer in `src/client/multiplexer.ts` free of the transformer: it runs in the shared worker, so inputs, data, and error shapes stay serialized, and the tab adapter serializes and deserializes them.
- Keep every subscription and its last event id in the tab adapter, so the tab can replay them into another transport.
- Bump `WORKER_PROTOCOL_VERSION` on any incompatible change of the messages between tabs and the worker.
- Keep the server accepting clients of the previous release: tabs opened before a deploy keep running the old client.
- Keep the restart channel name and its message unchanged across versions: restarts must reach workers and tabs of every release.

## Documentation

- Write public README and JSDoc text for package users who do not know the implementation.
- Add JSDoc to every exported declaration and to internal helpers whose contract, inputs, output, or failure behavior is not obvious.
- Add inline comments beside every non-obvious invariant, algorithmic choice, safety constraint, and intentionally limited behavior.
- Update nearby JSDoc and inline comments whenever the documented code changes, and remove comments that no longer apply.
- Do not narrate self-evident syntax or restate what a name already communicates.
- Do not document obvious or implied defaults.
- Describe a default only when readers need it to make a decision or avoid surprising behavior.
- Use One Sentence Per Line for connected prose.
- Keep semantically connected explanations as prose paragraphs.
- Use lists for separate assertions instead of presenting them as prose paragraphs.

## Changesets

- Add one `.changeset/*.md` file for each independently releasable user-visible change.
- Do not add changesets for internal refactors, maintenance, tests, or documentation changes that do not require a package release.
- Choose the SemVer bump from the public contract: `patch` for backward-compatible fixes and `minor` for backward-compatible functionality.
- Before 1.0, use `minor` for breaking changes; starting with 1.0, use `major` and remove this rule.
- Create `.changeset/<unique-name>.md` with this format:

```markdown
---
"trpc-multiplex": patch
---

Describe the user-visible change.
```

- Briefly describe the user-observable change or new capability in the public contract, without implementation details or rationale.
  Prefer a single sentence.
- Do not edit the package version or `CHANGELOG.md` by hand, and do not run `changeset version` or `changeset publish`; the release workflow consumes pending changesets.

## Tests

- Add a `describe` block where the file gives a reason for it: several APIs or behaviors in one file, or a fixture that belongs to some cases but not all.
  Name such a block after what it covers and keep its fixtures inside it.
- Distinguish several same-kind values by role rather than by order.
  When values differ only by order, number them with digits instead of ordinal words.
- Keep tests deterministic so a failure repeats on every run.
  Generate random inputs from an explicit seed and print the seed in failure messages so the failing input can be replayed.

## Checks

- Run the `types` script when public types or TypeScript configuration change.
- Run the `test` script when behavior changes.
- Run the `build` script when package exports, declarations, or supported runtimes change.
- Run the `test:e2e` script when the shared worker, the tab lifecycle, or the worker bundling contract changes.
