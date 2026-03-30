# Beam Integration Split

This repository is the standalone PoC half of a two-repository setup.

## Standalone Repo

- Repo: `Eliaaazzz/beam-native-streaming-poc`
- Purpose: isolated review of the modular PoC package, examples, and tests
- Branch: `main`

## Beam Integration Workspace

- Purpose: keep Beam-side wiring separate from the standalone PoC
- Beam branch: `integration/iobase`
- Focus: wire `beam.io.Read(UnboundedSource)` through the SDF wrapper and keep
  integration-specific review isolated from the standalone package

## Review Order

1. Review this repository first for API shape, tracker semantics, watermark
   behavior, and test coverage.
2. Review the Beam integration workspace second for SDK wiring only.
