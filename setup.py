from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sporc",
    version="1.0.0",
    author="David Jurgens",
    author_email="jurgens@umich.edu",
    description="A Python package for working with the SPORC (Structured Podcast Open Research Corpus) dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/davidjurgens/sporc",
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
        "huggingface_hub>=0.16.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "requests>=2.25.0",
        "tqdm>=4.62.0",
        "pyarrow>=12.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "isort>=5.0",
            "flake8>=3.8",
            "mypy>=0.910",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
        ],
        "duckdb": [
            "duckdb>=0.9.0",
        ],
        # Word-level alignment and formant measurement from source audio. Heavy
        # and optional: the corpus itself carries no word timings, so these are
        # only needed to re-derive them (see sporc/phonetics.py). Also requires
        # the ffmpeg binary on PATH.
        "phonetics": [
            "torch>=2.1",
            "torchaudio>=2.1",
            "transformers>=4.30",
            "praat-parselmouth>=0.4",
            "soundfile>=0.12",
            "nltk>=3.8",
        ],
    },
    entry_points={
        "console_scripts": [
            "sporc=sporc.cli:main",
        ],
    },
    keywords="podcast, audio, nlp, research, dataset, huggingface, streaming",
    project_urls={
        "Bug Reports": "https://github.com/davidjurgens/sporc/issues",
        "Source": "https://github.com/davidjurgens/sporc",
        "Documentation": "https://github.com/davidjurgens/sporc/wiki",
    },
)