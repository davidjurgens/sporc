# Installation

The SPoRC dataset is **gated**, so authentication is a required first step —
without it, downloads fail with an access error.

## 1. Accept the dataset terms

Visit the [dataset card](https://huggingface.co/datasets/blitt/SPoRC), log in to
your Hugging Face account, and click **Agree** to accept the terms of use.

## 2. Authenticate locally

```bash
pip install huggingface_hub
hf auth login
```

Paste a token from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)
when prompted.

!!! note "Older `huggingface_hub`"
    On versions before the CLI rename, this command was
    `huggingface-cli login`. Both write the same cached token.

## 3. Install the package

```bash
pip install sporc
```

Or from source:

```bash
git clone https://github.com/davidjurgens/sporc.git
cd sporc
pip install -e .
```

## Optional extras

| Extra | Install | Adds |
|---|---|---|
| Full-text search | `pip install "sporc[duckdb]"` | DuckDB, for the BM25 search index used by `search_turns`, `search_episodes_by_text`, and `concordance`. |
| Phonetics | `pip install "sporc[phonetics]"` | torch, torchaudio, transformers, parselmouth for word alignment and formant measurement. Also needs an `ffmpeg` binary on PATH. |
| Docs | `pip install "sporc[docs]"` | MkDocs Material + mkdocstrings, to build this site locally. |
| Dev | `pip install "sporc[dev]"` | pytest, black, isort, flake8, mypy. |

## Verify

```bash
python -c "import sporc; print(sporc.__version__)"
```

Then continue to the [Quick start](quickstart.md).
