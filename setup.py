from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sporc",
    version="0.1.7",
    author="SPORC Package Maintainer",
    author_email="maintainer@example.com",
    description="A Python package for working with the SPORC (Structured Podcast Open Research Corpus) dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/sporc",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Multimedia :: Sound/Audio :: Analysis",
        "Topic :: Text Processing :: Linguistic",
    ],
    python_requires=">=3.8",
    install_requires=[
        "datasets>=2.0.0",
        "huggingface_hub>=0.16.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "requests>=2.25.0",
        "tqdm>=4.62.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.910",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
        ],
        "streaming": [
            "psutil>=5.8.0",  # For memory monitoring in streaming examples
        ],
    },
    keywords="podcast, audio, nlp, research, dataset, huggingface, streaming",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/sporc/issues",
        "Source": "https://github.com/yourusername/sporc",
        "Documentation": "https://github.com/yourusername/sporc/wiki",
    },
)