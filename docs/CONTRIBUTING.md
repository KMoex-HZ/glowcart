# Contributing to GlowCart

Thanks for your interest in contributing. This document outlines the workflow for making changes to the GlowCart pipeline.

## Branch Naming
```
feature/   → new functionality (e.g. feature/add-schema-registry)
fix/       → bug fixes (e.g. fix/dlq-null-handling)
docs/      → documentation only (e.g. docs/update-runbook)
refactor/  → code restructuring without behavior change
```

## Workflow

1. Branch off from `main`
2. Make changes
3. Run tests locally before pushing:
```bash
   cd /root/glowcart
   pytest tests/ -v
   cd transform/dbt && dbt test
```
4. Open a Pull Request to `main`
5. CI must pass (GitHub Actions) before merging

## Adding a New Pipeline Stage

- Follow the existing Bronze/Silver/Gold pattern
- Idempotency check is mandatory — every stage must be safe to rerun
- Add structured logging using `utils/logger.py`, never `print()`
- If it's a new architectural decision, write an ADR in `docs/adr/`

## Adding a New ADR

Copy this template to `docs/adr/ADR-00X-short-title.md`:
```
# ADR-00X: Title

**Status:** Proposed | Accepted | Deprecated  
**Date:** YYYY-MM-DD  
**Author:** Your Name

## Context
## Decision
## Consequences
## Alternatives Considered
```

## Questions

Open a GitHub Issue with the `question` label.
