# Contributing to SPORC

Thank you for your interest in contributing to the SPORC package! This guide will help you get started with contributing to the project.

## Getting Started

### Prerequisites

Before contributing, make sure you have:

1. **Python 3.8 or higher** installed
2. **Git** installed
3. **A GitHub account** (for forking and submitting pull requests)
4. **Basic knowledge** of Python development

### Development Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/yourusername/sporc.git
   cd sporc
   ```

3. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install development dependencies**:
   ```bash
   pip install -e .[dev]
   ```

5. **Set up pre-commit hooks** (optional but recommended):
   ```bash
   pre-commit install
   ```

## Development Workflow

### 1. Create a Feature Branch

Always work on a new branch for your changes:

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/add-new-search-method`
- `bugfix/fix-memory-leak`
- `docs/update-installation-guide`

### 2. Make Your Changes

Follow these guidelines when making changes:

#### Code Style

- Follow **PEP 8** style guidelines
- Use **type hints** for all function parameters and return values
- Write **docstrings** for all public functions and classes
- Keep functions **small and focused** (under 50 lines when possible)
- Use **descriptive variable names**

#### Example of Good Code:

```python
from typing import List, Optional, Dict, Any

def search_episodes_by_duration(
    episodes: List[Episode],
    min_duration: Optional[float] = None,
    max_duration: Optional[float] = None
) -> List[Episode]:
    """
    Search for episodes within a specific duration range.

    Args:
        episodes: List of episodes to search through
        min_duration: Minimum duration in seconds (inclusive)
        max_duration: Maximum duration in seconds (inclusive)

    Returns:
        List of episodes matching the duration criteria

    Raises:
        ValueError: If min_duration is greater than max_duration
    """
    if min_duration is not None and max_duration is not None:
        if min_duration > max_duration:
            raise ValueError("min_duration cannot be greater than max_duration")

    matching_episodes = []
    for episode in episodes:
        duration = episode.duration_seconds

        if min_duration is not None and duration < min_duration:
            continue
        if max_duration is not None and duration > max_duration:
            continue

        matching_episodes.append(episode)

    return matching_episodes
```

#### Testing

- Write **unit tests** for all new functionality
- Write **integration tests** for complex features
- Ensure **100% test coverage** for new code
- Run tests before submitting: `pytest`

Example test:

```python
import pytest
from sporc import SPORCDataset, Episode

def test_search_episodes_by_duration():
    """Test searching episodes by duration criteria."""
    # Create mock episodes
    episodes = [
        Episode(title="Short", duration_seconds=300),
        Episode(title="Medium", duration_seconds=1800),
        Episode(title="Long", duration_seconds=3600)
    ]

    # Test minimum duration
    long_episodes = search_episodes_by_duration(episodes, min_duration=1800)
    assert len(long_episodes) == 2
    assert all(ep.duration_seconds >= 1800 for ep in long_episodes)

    # Test maximum duration
    short_episodes = search_episodes_by_duration(episodes, max_duration=1800)
    assert len(short_episodes) == 2
    assert all(ep.duration_seconds <= 1800 for ep in short_episodes)

    # Test range
    medium_episodes = search_episodes_by_duration(
        episodes, min_duration=600, max_duration=2700
    )
    assert len(medium_episodes) == 1
    assert medium_episodes[0].title == "Medium"
```

### 3. Documentation

- **Update docstrings** for any modified functions
- **Update README.md** if adding new features
- **Update wiki pages** if changing user-facing functionality
- **Add examples** for new features

### 4. Commit Your Changes

Use clear, descriptive commit messages:

```bash
git add .
git commit -m "feat: add duration-based episode search

- Add search_episodes_by_duration function
- Support min/max duration filtering
- Add comprehensive unit tests
- Update documentation with examples"
```

Follow conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions
- `refactor:` for code refactoring
- `style:` for formatting changes

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub with:

1. **Clear title** describing the change
2. **Detailed description** of what was changed and why
3. **Link to related issues** if applicable
4. **Screenshots** for UI changes
5. **Test results** showing all tests pass

## Areas for Contribution

### Code Improvements

- **Performance optimizations**
- **Memory usage improvements**
- **Error handling enhancements**
- **New search methods**
- **Additional data analysis features**

### Documentation

- **Improve existing documentation**
- **Add more examples**
- **Create tutorials**
- **Translate documentation** to other languages
- **Add API reference examples**

### Testing

- **Add more test cases**
- **Improve test coverage**
- **Add integration tests**
- **Add performance benchmarks**

### Examples and Tutorials

- **Create Jupyter notebooks**
- **Add research use case examples**
- **Create data analysis scripts**
- **Add visualization examples**

## Code Review Process

### Before Submitting

1. **Run all tests**: `pytest`
2. **Check code style**: `black .` and `flake8`
3. **Check type hints**: `mypy sporc/`
4. **Update documentation** if needed
5. **Test your changes** with real data

### Review Checklist

Your pull request should include:

- [ ] **Tests** for new functionality
- [ ] **Documentation** updates
- [ ] **Type hints** for all functions
- [ ] **Error handling** for edge cases
- [ ] **Performance considerations** for large datasets
- [ ] **Backward compatibility** (if applicable)

### Responding to Reviews

- **Address all comments** from reviewers
- **Make requested changes** promptly
- **Ask questions** if something is unclear
- **Thank reviewers** for their time

## Development Guidelines

### Error Handling

Always handle potential errors gracefully:

```python
def safe_search_podcast(dataset: SPORCDataset, name: str) -> Optional[Podcast]:
    """Safely search for a podcast with error handling."""
    try:
        return dataset.search_podcast(name)
    except SPORCError as e:
        logger.warning(f"Failed to search for podcast '{name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error searching for podcast '{name}': {e}")
        return None
```

### Performance Considerations

- **Use streaming mode** for large datasets
- **Implement caching** for expensive operations
- **Use generators** for memory efficiency
- **Profile code** before optimizing

### Security

- **Never commit secrets** or API keys
- **Validate user input** thoroughly
- **Use environment variables** for configuration
- **Follow security best practices**

## Testing Guidelines

### Unit Tests

- **Test one thing** per test function
- **Use descriptive test names**
- **Test edge cases** and error conditions
- **Mock external dependencies**

### Integration Tests

- **Test with real data** when possible
- **Test end-to-end workflows**
- **Test performance** with large datasets
- **Test error recovery**

### Test Structure

```python
class TestEpisodeSearch:
    """Test episode search functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dataset = SPORCDataset(streaming=True)
        self.dataset.load_podcast_subset(categories=['education'])

    def test_search_by_duration(self):
        """Test searching episodes by duration."""
        episodes = self.dataset.search_episodes(min_duration=1800)
        assert all(ep.duration_seconds >= 1800 for ep in episodes)

    def test_search_by_speaker_count(self):
        """Test searching episodes by speaker count."""
        episodes = self.dataset.search_episodes(min_speakers=2, max_speakers=3)
        for episode in episodes:
            assert 2 <= episode.speaker_count <= 3

    def test_invalid_search_parameters(self):
        """Test that invalid search parameters raise appropriate errors."""
        with pytest.raises(ValueError, match="min_duration cannot be greater"):
            self.dataset.search_episodes(min_duration=3600, max_duration=1800)
```

## Documentation Guidelines

### Docstrings

Use Google-style docstrings:

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
    pass
```

### README Updates

When updating the README:

- **Keep it concise** but informative
- **Update examples** if API changes
- **Add new features** to the feature list
- **Update installation** instructions if needed

### Wiki Documentation

When updating wiki pages:

- **Use clear headings** and structure
- **Include code examples**
- **Add troubleshooting sections**
- **Keep information current**

## Release Process

### Version Bumping

Follow semantic versioning:

- **Patch** (1.0.1): Bug fixes and minor improvements
- **Minor** (1.1.0): New features, backward compatible
- **Major** (2.0.0): Breaking changes

### Release Checklist

Before releasing:

- [ ] **All tests pass**
- [ ] **Documentation is updated**
- [ ] **Version number is bumped**
- [ ] **CHANGELOG is updated**
- [ ] **Release notes are written**

## Getting Help

### Questions and Discussion

- **GitHub Issues**: For bug reports and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Email**: For private or sensitive matters

### Mentorship

New contributors are welcome! Don't hesitate to:

- **Ask questions** in issues or discussions
- **Request code reviews** for learning
- **Start with small changes** like documentation
- **Join the community** discussions

## Code of Conduct

We are committed to providing a welcoming and inclusive environment for all contributors. Please:

- **Be respectful** and inclusive
- **Use welcoming language**
- **Be collaborative**
- **Focus on what is best for the community**
- **Show empathy** towards other community members

## Recognition

Contributors will be recognized in:

- **README.md** contributors section
- **Release notes** for significant contributions
- **GitHub contributors** page
- **Project documentation**

Thank you for contributing to SPORC! Your contributions help make this project better for everyone in the research community.