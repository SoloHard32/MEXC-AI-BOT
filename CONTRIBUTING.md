# Contributing

Thanks for contributing to **MEXC AI BOT v1.0**.

## Development setup

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
pip install pytest
```

3. Run tests:

```bash
pytest -q
```

## Rules

- Never commit secrets (`.env`, API keys, private data).
- Keep changes focused and small.
- Add/update tests for behavioral fixes.
- Preserve existing core trade logic unless explicitly changing it.

## Commit style

Use clear commit messages, for example:

- `fix(gui): prevent stale live status render`
- `feat(ai): add adaptive threshold smoothing`

## Pull requests

- Describe the problem and the fix.
- Include verification steps.
- Attach screenshots for UI changes.
