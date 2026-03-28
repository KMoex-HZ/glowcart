# ADR-007: Python 3.12 as the Runtime Version

- **Status:** Accepted
- **Date:** 2026-03-25
- **Author:** Khairunnisa Maharani

---

## Context

GlowCart's pipeline is written in Python. Choosing a Python version affects performance, available language features, library compatibility, and alignment with what production data engineering teams are currently running.

At the time this project was built, the active Python releases were 3.10, 3.11, and 3.12. Python 3.9 and below are end-of-life or approaching it.

---

## Decision

Use **Python 3.12** across the entire project — pipeline scripts, unit tests, and the FastAPI serving layer.

The version is pinned in `.python-version` (for pyenv users) and specified in `requirements.txt` comments.

---

## Consequences

**Positive:**
- Python 3.12 delivers meaningful performance improvements over 3.10/3.11 — CPython interpreter is approximately 5% faster on average, with more significant gains in specific workloads (loop-heavy code, function calls).
- `f-string` improvements (nested expressions, reuse of quotes) make log formatting cleaner.
- Better error messages with more precise tracebacks — reduces debugging time.
- Aligned with what production teams are adopting in 2024–2025; shows awareness of the current ecosystem.
- Full compatibility with all libraries used in this project: PySpark 4.1, dbt 1.11, Airflow 2.9, Great Expectations, FastAPI.

**Negative / Trade-offs:**
- PySpark officially supports Python 3.12 from Spark 3.5+ onward. This project uses Spark 4.1, so there is no compatibility issue — but it is a constraint worth being aware of for anyone backporting to older Spark versions.
- Some older internal tooling at companies may still pin to 3.10 or 3.11. Minor syntax differences (e.g., `tomllib` stdlib addition) are rarely breaking but worth noting.

---

## Alternatives Considered

**Python 3.11**
Introduced the largest single-version performance improvement in CPython history (~25% faster than 3.10 in some benchmarks). A strong alternative. Ultimately 3.12 was chosen because it is the current stable release and the direction the ecosystem is moving.

**Python 3.10**
Still widely used in production, especially at companies slow to upgrade. However, it lacks several quality-of-life improvements in error messages and type hints that make development smoother. Not chosen.

**Python 3.9 or below**
End-of-life or approaching it. Not considered.

---

## Notes

If deploying this pipeline to a managed environment (e.g., AWS EMR, Cloud Composer), verify that the managed service supports Python 3.12 before migrating. As of 2025, most managed Airflow and Spark services support 3.11–3.12.