# Contributing to SPoRC

This guide covers getting set up and landing a change.

## Getting started

### Prerequisites

Before contributing, make sure you have:

1. **Python 3.8 or higher** installed
2. **Git** installed
3. **A GitHub account** (for forking and submitting pull requests)
4. **Basic knowledge** of Python development

### Development setup

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:

   ```bash
   git clone https://github.com/davidjurgens/sporc.git
   cd sporc
   ```

3. **Create a virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install the package with its development dependencies.** Packaging is
   defined in `pyproject.toml`, and the dev extra pulls in the test and linting
   tools:

   ```bash
   pip install -e ".[dev]"
   ```

   To build or preview the documentation, also install the docs extra:

   ```bash
   pip install -e ".[docs]"
   ```

5. **Set up pre-commit hooks** (optional but recommended):

   ```bash
   pre-commit install
   ```

## Development workflow

### 1. Create a feature branch

Always work on a new branch for your changes:

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:

- `feature/add-new-search-method`
- `bugfix/fix-memory-leak`
- `docs/update-installation-guide`

### 2. Make your changes

Follow these guidelines when making changes:

#### Code style

- Follow **PEP 8** style guidelines.
- Use **type hints** for all function parameters and return values.
- Write **docstrings** for all public functions and classes (see
  [Docstrings](#docstrings) below).
- Keep functions **small and focused** (under 50 lines when possible).
- Use **descriptive variable names**.

The project uses a standard tool set, all configured in `pyproject.toml`:

```bash
black sporc/ tests/     # format
isort sporc/ tests/     # sort imports
flake8 sporc/ tests/    # lint
mypy sporc/             # type-check
```

#### Testing

Write **unit tests** for all new functionality and **integration tests** for
complex features. Aim to keep new code well covered, and run the suite before
submitting:

```bash
pytest                                      # full suite
pytest -m "not slow and not integration"    # fast subset
pytest --cov=sporc                          # with coverage
```

Slow and network-dependent tests are marked so you can skip them while iterating;
run the full suite (and coverage) before opening a pull request.

Example test:

```python
import pytest
from sporc import SPORCDataset, Episode

def test_search_episodes_by_duration():
    """Test searching episodes by duration criteria."""
    episodes = [
        Episode(title="Short", duration_seconds=300),
        Episode(title="Medium", duration_seconds=1800),
        Episode(title="Long", duration_seconds=3600),
    ]

    long_episodes = search_episodes_by_duration(episodes, min_duration=1800)
    assert len(long_episodes) == 2
    assert all(ep.duration_seconds >= 1800 for ep in long_episodes)

    short_episodes = search_episodes_by_duration(episodes, max_duration=1800)
    assert len(short_episodes) == 2
    assert all(ep.duration_seconds <= 1800 for ep in short_episodes)
```

### 3. Documentation

- **Update docstrings** for any modified functions.
- **Update the docs** under `docs/` if you change user-facing behavior.
- **Update `README.md`** if adding a headline feature.
- **Add examples** for new features.

See [Building the docs locally](#building-the-docs-locally) below to preview your
changes.

### 4. Commit your changes

Use clear, descriptive commit messages that follow the conventional-commit
format:

```bash
git add .
git commit -m "feat: add duration-based episode search

- Add search_episodes_by_duration function
- Support min/max duration filtering
- Add comprehensive unit tests
- Update documentation with examples"
```

Common prefixes:

- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions
- `refactor:` for code refactoring
- `style:` for formatting changes

### 5. Push and open a pull request

```bash
git push origin feature/your-feature-name
```

Then open a pull request against
[`github.com/davidjurgens/sporc`](https://github.com/davidjurgens/sporc) with:

1. A **clear title** describing the change.
2. A **detailed description** of what changed and why.
3. A **link to related issues** if applicable.
4. **Test results** showing the suite passes.

## Building the docs locally

The documentation is a [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
site, with API pages generated from docstrings by
[mkdocstrings](https://mkdocstrings.github.io/). After installing the docs extra
(`pip install -e ".[docs]"`):

```bash
mkdocs serve            # live-reloading preview at http://127.0.0.1:8000
mkdocs build            # full build into ./site (what Read the Docs runs)
```

Run `mkdocs build` before submitting doc changes and check the output: the
`validation:` config in `mkdocs.yml` reports broken cross-references and missing
pages as warnings. (`mkdocs build --strict` turns every warning into an error,
including cosmetic mkdocstrings/griffe notes about docstrings that lack type
annotations, so it is stricter than Read the Docs itself.)

## Docstrings

Public functions and classes use **Google-style** docstrings (`Args:`,
`Returns:`, `Raises:` sections), and mkdocstrings renders them directly into the
[API reference](reference/index.md). New public methods should follow the same
style so they show up correctly:

```python
def complex_function(param1: str, param2: Optional[int] = None) -> List[str]:
    """Brief description of what the function does.

    Longer description if needed, explaining the function's purpose,
    behavior, and any important details.

    Args:
        param1: Description of the first parameter.
        param2: Description of the second parameter. Defaults to None.

    Returns:
        Description of what the function returns.

    Raises:
        ValueError: Description of when this error is raised.
        SPORCError: Description of when this error is raised.

    Example:
        >>> result = complex_function("example", 42)
        >>> print(result)
        ['example_42']
    """
    ...
```

## Areas for contribution

**Code:** performance and memory improvements, better error handling, new search
methods, additional analysis features.

**Documentation:** improve existing pages, add examples, write tutorials, expand
the API reference.

**Testing:** add test cases, improve coverage, add integration tests and
benchmarks.

**Examples:** contribute Jupyter notebooks and research use-case walkthroughs.

## Development guidelines

### Error handling

Handle potential errors gracefully, and prefer the package's own exception
hierarchy (`SPORCError` and its subclasses) so callers can catch them uniformly:

```python
def safe_search_podcast(dataset: SPORCDataset, name: str) -> Optional[Podcast]:
    """Safely search for a podcast with error handling."""
    try:
        return dataset.search_podcast(name)
    except SPORCError as e:
        logger.warning(f"Failed to search for podcast '{name}': {e}")
        return None
```

### Security

- **Never commit secrets** or API keys.
- **Validate user input** thoroughly.
- **Use environment variables** for configuration.

## Release process

The project follows semantic versioning:

- **Patch** (1.0.1): bug fixes and minor improvements
- **Minor** (1.1.0): new features, backward compatible
- **Major** (2.0.0): breaking changes

Before releasing, confirm that all tests pass, the docs build cleanly
(`mkdocs build`), the version number is bumped in `pyproject.toml` and
`sporc/__init__.py`, and the changelog is updated.

## Getting help

- **GitHub Issues**: for bug reports and feature requests
- **GitHub Discussions**: for questions and general discussion

New contributors are welcome. Ask questions in issues or discussions, request
reviews to learn, and start with small changes like documentation.

## Code of conduct

We are committed to providing a welcoming and inclusive environment for all
contributors. Please be respectful, use welcoming language, be collaborative,
focus on what is best for the community, and show empathy toward other community
members.
