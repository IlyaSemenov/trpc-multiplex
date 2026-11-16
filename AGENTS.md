# trpc-multiplex Agent Guide

## Overview

Multiplex multiple tRPC subscriptions over a single HTTP stream.

Read [README.md](README.md) completely before changing the public API, package behavior, supported runtimes, or user documentation.

Read the relevant guide in [development/](development/README.md) before starting:

- [user-documentation.md](development/user-documentation.md) before changing user documentation.

Extend this guide only with stable, non-obvious conventions, architecture, contracts, workflows, and gotchas that most changes need; put those of one topic in a guide in development/, and list the guide in its index and above with the task that needs it.
Do not catalog files or restate information evident from their names and locations.

## Scope

- Keep production code in `src/`.
- Name a directory of one concept in the singular, such as `parser/`, and a directory of like items in the plural, such as `rules/`.
- Test behavior through the public entry points whenever possible: keep such integration, package-boundary, and type-inference tests in `tests/`, importing the package by name as mapped by `tsconfig.json` paths.
- Keep tests that need internals of one module, such as the wire protocol of the server, beside their source as `*.test.ts`.
- Keep tests that span several modules and use internals in `src/__tests__/` with relative imports.
- Reuse the test server and router of `tests/harness.ts` in tests under `src/` instead of duplicating them.
- Name compile-only tests `*.type-test.ts`.
- Keep `src/client/index.ts`, `src/server/index.ts`, and `src/worker/index.ts` limited to explicit public exports.
- Export from each of them every type that the inferred types of its public values reference, so that the declarations of a consumer module can name them.
- Keep browser tests in `e2e/` as `*.e2e.ts`; they run the built package in a Vite app, in both the dev server and the production build.
- Treat `package.json` exports and supported runtimes as public contracts.

## Architecture

- Keep the multiplexer in `src/client/multiplexer.ts` free of the transformer: it runs in the shared worker, so inputs, data, and error shapes stay serialized, and the tab adapter serializes and deserializes them.
- Keep every subscription and its last event id in the tab adapter, so the tab can replay them into another transport.
- Bump `WORKER_PROTOCOL_VERSION` on any incompatible change of the messages between tabs and the worker.
- Keep the server accepting clients of the previous release: tabs opened before a deploy keep running the old client.
- Keep the restart channel name and its message unchanged across versions: restarts must reach workers and tabs of every release.

## Design

- With every change, look for refactorings that reduce the complexity of the code it touches, such as duplicated logic or a concept spread over several modules.
  Make those that ease the change, and propose the others in the final report, listed apart from its fixes.
- Give a domain rule or contract a function of its own at its first use, such as how a cache key is built or which values an option accepts, so that the next use finds it; give it a module of its own only when several modules use it.
  Do not extract code that only looks alike.
- Before adding a helper, search for an existing one that does the same.

## Refactoring

- When moving or splitting code, move its tests to follow it.

## Documentation

- Write public README and JSDoc text for package users who do not know the implementation.
- When a change makes an existing statement false, fix that statement rather than add an explanation next to it.
- Add JSDoc to every exported declaration and to internal helpers whose contract, inputs, output, or failure behavior is not obvious.
- Add inline comments beside every non-obvious invariant, algorithmic choice, safety constraint, and intentionally limited behavior.
- Update nearby JSDoc and inline comments whenever the documented code changes, and remove comments that no longer apply.
- Do not narrate self-evident syntax or restate what a name already communicates.
- Do not document obvious or implied defaults.
- Describe a default only when readers need it to make a decision or avoid surprising behavior.
- Use One Sentence Per Line for connected prose, including JSDoc and code comments: do not wrap a sentence across lines.
- Keep semantically connected explanations as prose paragraphs, and use lists for separate assertions.
- Link a file of another repository by a tag or a commit, such as of the version you checked, never by a branch such as `main`, whose file changes.

## Changesets

- Add changesets as [.changeset/README.md](.changeset/README.md) describes.

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
