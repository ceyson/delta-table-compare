"""
Cross-platform environment setup script.

Works on Windows (cmd/PowerShell/VSCode terminal), Linux, and macOS.
Usage:
    python setup_env.py          # Full setup (Spark + Polars)
    python setup_env.py --polars # Polars-only (no PySpark/Java dependency)
"""

import os
import platform
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
VENV_DIR = SCRIPT_DIR / ".venv"
REQUIREMENTS_DEV = SCRIPT_DIR / "requirements-dev.txt"

IS_WINDOWS = platform.system() == "Windows"

# On Windows, venv puts executables in Scripts/ not bin/
if IS_WINDOWS:
    VENV_PYTHON = VENV_DIR / "Scripts" / "python.exe"
    VENV_PIP = VENV_DIR / "Scripts" / "pip.exe"
else:
    VENV_PYTHON = VENV_DIR / "bin" / "python"
    VENV_PIP = VENV_DIR / "bin" / "pip"


def run(cmd: list[str], **kwargs) -> None:
    print(f"  > {' '.join(cmd)}")
    subprocess.check_call(cmd, **kwargs)


def create_venv() -> None:
    if not VENV_DIR.exists():
        print(f"Creating venv at {VENV_DIR}...")
        run([sys.executable, "-m", "venv", str(VENV_DIR)])
    else:
        print(f"Venv already exists at {VENV_DIR}")


def install_deps(polars_only: bool = False) -> None:
    print("\nUpgrading pip...")
    run([str(VENV_PYTHON), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel", "-q"])

    if polars_only:
        print("\nInstalling Polars-only dependencies (no PySpark)...")
        packages = [
            "pyarrow>=14.0",
            "polars>=1.0",
            "deltalake>=0.18",
            "pytest>=7.0",
            "pytest-timeout>=2.2",
        ]
        run([str(VENV_PYTHON), "-m", "pip", "install"] + packages + ["-q"])
    else:
        print(f"\nInstalling all dependencies from {REQUIREMENTS_DEV.name}...")
        run([str(VENV_PYTHON), "-m", "pip", "install", "-r", str(REQUIREMENTS_DEV), "-q"])

    # Install the project itself in editable mode
    print("\nInstalling recon package (editable)...")
    run([str(VENV_PYTHON), "-m", "pip", "install", "-e", str(SCRIPT_DIR), "-q"])


def print_instructions(polars_only: bool) -> None:
    activate = (
        str(VENV_DIR / "Scripts" / "activate") if IS_WINDOWS
        else f"source {VENV_DIR / 'bin' / 'activate'}"
    )
    test_cmd = 'pytest tests/ -m "not spark" -v' if polars_only else "pytest tests/ -v"

    print("\n" + "=" * 60)
    print("Environment ready!")
    print("=" * 60)
    print(f"\n  Activate:  {activate}")
    print(f"  Run tests: {test_cmd}")
    if polars_only:
        print("\n  NOTE: Spark tests excluded (--polars mode).")
        print("  To include Spark tests, re-run: python setup_env.py")
    print()


def main() -> None:
    polars_only = "--polars" in sys.argv

    if polars_only:
        print("=== Polars-only setup (no PySpark/Java required) ===\n")
    else:
        print("=== Full setup (Spark + Polars) ===\n")

    create_venv()
    install_deps(polars_only=polars_only)
    print_instructions(polars_only=polars_only)


if __name__ == "__main__":
    main()
