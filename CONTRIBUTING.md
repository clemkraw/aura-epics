# Contributing to AURA-EPICS

Thank you for your interest in contributing to AURA. This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Commit Convention](#commit-convention)
- [Pull Request Process](#pull-request-process)
- [Testing](#testing)
- [Documentation](#documentation)
- [License](#license)

## Code of Conduct

AURA is developed in the context of scientific research. We are committed to providing a welcoming and inclusive environment for everyone, regardless of background or experience level.

Please be respectful, constructive, and professional in all interactions.

## Getting Started

AURA is a high performance, entropy-driven archiving engine for EPICS control systems, written in Rust. Before contributing, we recommend:

1. Reading the [project paper](docs/aura-project.pdf) to understand the architecture and goals
2. Familiarizing yourself with [EPICS](https://epics-controls.org/) and the [Channel Access protocol](https://docs.epics-controls.org/en/latest/pv-access/protocol.html)
3. Having a working knowledge of Rust and async programming with [Tokio](https://tokio.rs/)

## Development Setup

### Prerequisites

- **Rust** >= 1.75 (install via [rustup](https://rustup.rs/))
- **EPICS Base** >= 7.0 (for testing against real IOCs)
- **PostgreSQL** >= 16 (with [TimescaleDB](https://github.com/timescale/timescaledb) extension)
- **Redis** >= 8.0 (used as message broker and real-time cache)

## How to Contribute

### Reporting Bugs

Open an issue on GitHub with:

- A clear, descriptive title
- Steps to reproduce the behavior
- Expected vs. actual behavior
- AURA version, Rust version, OS
- Relevant log output (with `RUST_LOG=aura=debug`)

### Suggesting Features

Open an issue with the `enhancement` label. Describe:

- The problem your feature would solve
- Your proposed approach
- Any alternatives you considered

### Submitting Code

1. Fork the repository
2. Create a feature branch from `main` (`git checkout -b feat/my-feature`)
3. Make your changes following the [coding standards](#coding-standards)
4. Add or update tests as appropriate
5. Run the full test suite (`cargo test`)
6. Run the linter (`cargo clippy -- -D warnings`)
7. Run the formatter (`cargo fmt`)
8. Submit a pull request

## Coding Standards

### Rust Style

- Follow the official [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Run `cargo fmt` before every commit — no exceptions
- Run `cargo clippy -- -D warnings` and resolve all warnings
- All public items must have doc comments (`///`)

### Error Handling

- Use `thiserror` for library error types, `anyhow` in the binary
- Never use `.unwrap()` in library code — propagate errors with `?`
- `.unwrap()` is acceptable only in tests and in cases with a comment explaining the invariant

### Async Code

- All I/O-bound operations must be async (Tokio)
- Use `tokio::sync::mpsc` for inter-module communication
- Never hold a `Mutex` or `RwLock` guard across an `.await` point
- Prefer `DashMap` over `Arc<RwLock<HashMap>>` for concurrent maps

### Safety

- No `unsafe` code without prior discussion and approval in a GitHub issue
- All shared state must be `Send + Sync` — the compiler enforces this, do not circumvent it
- Prefer owned data over references when crossing task boundaries

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

**Types:**

| Type       | Description                                      |
|------------|--------------------------------------------------|
| `feat`     | New feature                                      |
| `fix`      | Bug fix                                          |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `docs`     | Documentation only                               |
| `test`     | Adding or updating tests                         |
| `perf`     | Performance improvement                          |
| `ci`       | CI/CD configuration                              |
| `chore`    | Build process, dependencies, tooling             |

**Scopes:** `net`, `discover`, `engine`, `store`, `api`, `config`, `docker`

**Examples:**

```
feat(engine): implement windowed entropy computation
fix(net): handle malformed CA beacon packets gracefully
docs(store): add key schema documentation
test(engine): add segmenter regime-change detection tests
perf(store): batch RocksDB writes with WriteBatch
```

## Pull Request Process

1. **One concern per PR.** Don't mix a bug fix with a refactor.
2. **Describe what and why.** The PR description should explain the change, not just list files.
3. **Link related issues.** Use `Closes #42` or `Relates to #17`.
4. **All CI checks must pass.** This includes `cargo test`, `cargo clippy`, `cargo fmt --check`.
5. **Request review** from at least one maintainer.
6. **Squash commits** before merging if the history is noisy. Keep meaningful commits if each one is self-contained.

### PR Template

```markdown
## What

Brief description of the change.

## Why

What problem does this solve? Link to issue if applicable.

## How

Key implementation details or design decisions.

## Testing

How was this tested? New tests added?

## Checklist

- [ ] `cargo fmt` applied
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo test` passes
- [ ] Documentation updated (if applicable)
- [ ] No `unsafe` code added (or discussed and approved)
```

## Testing

### Running Tests

```bash
# All tests
cargo test

# Tests for a specific crate
cargo test -p aura-engine

# A specific test
cargo test -p aura-engine segmenter::tests::detect_regime_change

# With output
cargo test -- --nocapture
```

### Writing Tests

- Every public function should have at least one unit test
- Place unit tests in a `#[cfg(test)] mod tests` block at the bottom of the file
- Integration tests go in `tests/` at the workspace root
- Use descriptive test names: `test_segmenter_closes_segment_on_entropy_spike`
- Test edge cases: empty windows, NaN/infinity values, zero-variance signals

### Test Fixtures

Realistic PV signal data for testing is in `test/fixtures/`. To generate new fixtures:

```bash
cargo run --bin generate-fixtures -- --pvs 10 --duration 3600 --output test/fixtures/
```

## Documentation

- **Code docs:** Use `///` for public items. Run `cargo doc --open` to verify rendering.
- **Architecture docs:** Kept in `docs/` as Markdown files.
- **README:** Keep the root README concise — link to detailed docs rather than duplicating.
- **Changelog:** Maintained in `CHANGELOG.md` following [Keep a Changelog](https://keepachangelog.com/) format.

## License

By contributing to AURA-EPICS, you agree that your contributions will be licensed under the [MIT License](LICENSE).

---

Questions? Open a [discussion](https://github.com/clemkraw/aura-epics/discussions) or reach out to the maintainers.