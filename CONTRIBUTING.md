# Contributing

Bug reports and pull requests are welcome.

```bash
git clone https://github.com/DTheDataGuy/rowgate && cd rowgate
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q && ruff check .
```

Before you open a pull request:

- Add a test that fails without your change.
- Keep the core dependency list at `polars` only.
- New checks need a clear `expectation` string and an `observed` value a person can act on, such as "3 duplicate keys" rather than "False".
- Add a line to `CHANGELOG.md`.
