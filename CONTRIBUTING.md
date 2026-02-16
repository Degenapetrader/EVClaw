# Contributing to EVClaw

Thank you for your interest in contributing to EVClaw! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Style Guidelines](#style-guidelines)

## Code of Conduct

Be respectful and constructive. We welcome contributions from everyone.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a feature branch from `main`
4. Make your changes
5. Submit a pull request

## Development Setup

### Prerequisites

- Python 3.10+
- Access to Lighter and/or Hyperliquid APIs
- Tracker API access (EVCLAW_TRACKER_BASE_URL)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/evclaw.git
cd evclaw

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
# Edit .env with your credentials
```

### Configuration

1. Edit `skill.yaml` for trading parameters
2. Set environment variables in `.env`
3. See `docs/CONFIGURATION.md` for detailed options

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/add-new-signal-type`
- `fix/position-calculation-bug`
- `docs/update-readme`

### Commit Messages

Follow conventional commits:
- `feat: add support for new exchange`
- `fix: correct position sizing calculation`
- `docs: update configuration reference`
- `refactor: simplify signal parsing logic`

### Code Style

- Follow PEP 8 for Python code
- Use type hints where appropriate
- Keep functions focused and testable
- Add docstrings for public functions

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_trading_brain.py

# Run with coverage
pytest --cov=. tests/
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_<module>.py`
- Use descriptive test function names
- Test edge cases and error conditions

## Pull Request Process

1. **Update documentation** if changing behavior
2. **Add tests** for new functionality
3. **Ensure all tests pass** before submitting
4. **Keep PRs focused** - one feature/fix per PR
5. **Write clear PR descriptions** explaining the change

### PR Checklist

- [ ] Code follows style guidelines
- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Commit messages are clear
- [ ] PR description explains the change

## Style Guidelines

### Python

```python
# Use type hints
def calculate_position_size(conviction: float, equity: float) -> float:
    """Calculate position size based on conviction and equity."""
    ...

# Keep functions focused
def validate_signal(signal: dict) -> bool:
    """Validate signal has required fields."""
    required = ['symbol', 'z_score', 'direction']
    return all(k in signal for k in required)
```

### YAML Configuration

- Use 2-space indentation
- Group related settings
- Add comments for non-obvious values

```yaml
# Risk management settings
risk:
  starting_equity: 10000.0  # USD
  max_risk_pct: 2.5         # Per trade
```

## Questions?

Open an issue for:
- Bug reports
- Feature requests
- Questions about the codebase

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
