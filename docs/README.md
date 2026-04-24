# Dalgo Tech Docs

Documentation for the Dalgo platform backend using Zensical.

## Setup

### Prerequisites
- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

1. Navigate to the docs directory:
   ```bash
   cd docs
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

## Development

### Serve locally
```bash
uv run zensical serve
```

The documentation will be available at http://localhost:10001

### Build static site
```bash
uv run zensical build
```

The built site will be in the `site/` directory.

## Structure

```
docs/
├── README.md           # This file
├── pyproject.toml      # Python dependencies
├── zensical.toml       # Zensical configuration
├── uv.lock            # Dependency lock file
├── docs/              # Documentation content
│   ├── index.md       # Homepage
│   └── features/      # Feature documentation
└── site/              # Generated site (gitignored)
```

## Adding Documentation

1. Add markdown files to the `docs/` directory
2. Update navigation in `zensical.toml` if needed
3. The site will auto-reload when running `zensical serve`