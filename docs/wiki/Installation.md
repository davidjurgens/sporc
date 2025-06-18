# Installation Guide

This guide will walk you through the complete installation process for the SPORC package.

## Prerequisites

Before installing the SPORC package, you need to complete two important steps:

### 1. Accept the Dataset Terms

The SPORC dataset requires you to accept specific terms of use before accessing the data.

1. Visit the [SPORC dataset page](https://huggingface.co/datasets/blitt/SPoRC) on Hugging Face
2. Log in to your Hugging Face account (or create one if you don't have one)
3. Click the "I agree" button to accept the dataset terms
4. You should see a confirmation that you've accepted the terms

**Important**: You must complete this step before the package can access the dataset.

### 2. Set up Hugging Face Authentication

The package needs to authenticate with Hugging Face to download the dataset.

#### Option A: Using the Hugging Face CLI (Recommended)

1. Install the Hugging Face Hub package:
   ```bash
   pip install huggingface_hub
   ```

2. Login to Hugging Face:
   ```bash
   huggingface-cli login
   ```

3. Enter your Hugging Face token when prompted. You can find your token at [https://huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

#### Option B: Using Environment Variables

1. Get your Hugging Face token from [https://huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

2. Set the environment variable:
   ```bash
   # On Linux/Mac
   export HUGGING_FACE_HUB_TOKEN=your_token_here

   # On Windows (Command Prompt)
   set HUGGING_FACE_HUB_TOKEN=your_token_here

   # On Windows (PowerShell)
   $env:HUGGING_FACE_HUB_TOKEN="your_token_here"
   ```

## Installing the SPORC Package

### Option 1: Install from PyPI (Recommended)

```bash
pip install sporc
```

### Option 2: Install from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sporc.git
   cd sporc
   ```

2. Install in development mode:
   ```bash
   pip install -e .
   ```

### Option 3: Install with Additional Dependencies

For development or advanced usage, you might want to install additional dependencies:

```bash
# Install with development dependencies
pip install sporc[dev]

# Install with documentation dependencies
pip install sporc[docs]

# Install with all extra dependencies
pip install sporc[dev,docs]
```

## System Requirements

### Python Version

- **Python 3.8 or higher** is required
- The package is tested on Python 3.8, 3.9, 3.10, and 3.11

### Dependencies

The package automatically installs these required dependencies:

- `datasets>=2.0.0`: For loading Hugging Face datasets
- `huggingface_hub>=0.16.0`: For Hugging Face authentication and API access
- `pandas>=1.3.0`: For data manipulation
- `numpy>=1.21.0`: For numerical operations
- `requests>=2.25.0`: For HTTP requests
- `tqdm>=4.62.0`: For progress bars

### Optional Dependencies

For development and advanced usage:

- `pytest>=6.0`: For testing
- `black>=21.0`: For code formatting
- `flake8>=3.8`: For linting
- `mypy>=0.910`: For type checking
- `sphinx>=4.0`: For documentation generation

## Verification

After installation, you can verify that everything is working correctly:

```python
from sporc import SPORCDataset

# This should work without errors if installation was successful
print("SPORC package imported successfully!")
```

## Troubleshooting

### Common Issues

#### 1. Authentication Error

**Error**: `AuthenticationError: Authentication failed`

**Solution**:
- Make sure you've accepted the dataset terms on Hugging Face
- Verify your Hugging Face token is correct
- Try running `huggingface-cli login` again

#### 2. Dataset Access Error

**Error**: `DatasetAccessError: Dataset not found`

**Solution**:
- Ensure you've accepted the dataset terms
- Check your internet connection
- Verify the dataset ID is correct

#### 3. Import Error

**Error**: `ImportError: No module named 'datasets'`

**Solution**:
- Install the required dependencies: `pip install datasets huggingface_hub`
- Or reinstall the package: `pip install --force-reinstall sporc`

#### 4. Permission Error

**Error**: `PermissionError: [Errno 13] Permission denied`

**Solution**:
- Use a virtual environment
- Install with `--user` flag: `pip install --user sporc`
- Check file permissions in your cache directory

### Getting Help

If you encounter issues not covered here:

1. Check the [FAQ](FAQ.md) page for common solutions
2. Search existing [issues](https://github.com/yourusername/sporc/issues)
3. Create a new issue with:
   - Your operating system and Python version
   - Complete error message
   - Steps to reproduce the issue
   - Code example

## Next Steps

After installation, you can:

1. Read the [Basic Usage](Basic-Usage.md) guide
2. Try the [Search Examples](Search-Examples.md) for advanced usage
3. Explore the [API Reference](API-Reference.md)

## Uninstalling

To uninstall the SPORC package:

```bash
pip uninstall sporc
```

This will remove the package but keep your cached dataset files. To remove cached data as well, delete the Hugging Face cache directory (usually `~/.cache/huggingface/` on Linux/Mac or `%USERPROFILE%\.cache\huggingface\` on Windows).