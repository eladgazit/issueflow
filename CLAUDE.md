# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

The repo is freshly scaffolded ("Scaffold issueflow"). `README.md` and `DESIGN.md` are empty, and there is no `src/` or `tests/` directory yet. Update this file as the architecture takes shape.

## Project setup

- Python package `issueflow` (>=3.12), source layout under `src/` (pytest is configured with `pythonpath = ["src"]`, `testpaths = ["tests"]`).
- Console entry point: `issueflow = "issueflow.cli:main"` (so the CLI lives in `src/issueflow/cli.py`).
- A local virtualenv is expected at `.venv/`.

## Commands

- Run tests: `.venv/bin/python -m pytest`
- Run a single test: `.venv/bin/python -m pytest tests/test_x.py::test_name`
- No linter, formatter, or build tooling is configured yet.

## Conventions

- Python 3.12, standard library only plus pytest. Do not add new dependencies without asking first.
- Use type hints throughout.
- Keep modules small and focused.
- Run pytest after every change.
- `.gitignore` excludes `.venv/`, `out/` (generated output), `*.db` (local databases), and `.env` (secrets).
