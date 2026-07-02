"""Idempotent Jupyter kernel setup for local .ipynb notebooks (Windows)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VENV_PYTHON = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
KERNEL_NAME = "finledger-venv"
KERNEL_DISPLAY = "FinLedger (.venv)"


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def register_kernel() -> None:
    if not VENV_PYTHON.is_file():
        raise SystemExit(
            f"Missing {VENV_PYTHON}. Run: cd day7 && orchestrate.cmd"
        )
    _run(
        [
            str(VENV_PYTHON),
            "-m",
            "ipykernel",
            "install",
            "--user",
            f"--name={KERNEL_NAME}",
            f"--display-name={KERNEL_DISPLAY}",
        ]
    )


def write_vscode_settings() -> None:
    vscode_dir = REPO_ROOT / ".vscode"
    vscode_dir.mkdir(exist_ok=True)
    settings_path = vscode_dir / "settings.json"
    settings: dict = {}
    if settings_path.is_file():
        settings = json.loads(settings_path.read_text(encoding="utf-8"))

    settings["python.defaultInterpreterPath"] = "${workspaceFolder}\\.venv\\Scripts\\python.exe"
    settings["jupyter.jupyterServerType"] = "local"
    settings["notebook.lineNumbers"] = "on"
    settings["jupyter.kernels.excludePythonVersions"] = []
    settings["notebook.kernelProviderAssociations"] = {
        "*.ipynb": "ms-toolsai.jupyter"
    }

    settings_path.write_text(
        json.dumps(settings, indent=2) + "\n",
        encoding="utf-8",
    )


def pin_test_notebook() -> None:
    nb_path = REPO_ROOT / "day7" / "notebooks" / "test.ipynb"
    if not nb_path.is_file():
        return
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    nb.setdefault("metadata", {})
    nb["metadata"]["kernelspec"] = {
        "display_name": KERNEL_DISPLAY,
        "language": "python",
        "name": KERNEL_NAME,
    }
    nb_path.write_text(json.dumps(nb, indent=2) + "\n", encoding="utf-8")


def smoke_execute_test_notebook() -> None:
    nb_path = REPO_ROOT / "day7" / "notebooks" / "test.ipynb"
    if not nb_path.is_file():
        return
    _run(
        [
            str(VENV_PYTHON),
            "-m",
            "jupyter",
            "nbconvert",
            "--execute",
            "--to",
            "notebook",
            "--inplace",
            str(nb_path),
            f"--ExecutePreprocessor.kernel_name={KERNEL_NAME}",
        ]
    )


def main() -> None:
    print("Setting up Jupyter kernel for local notebooks...")
    register_kernel()
    write_vscode_settings()
    pin_test_notebook()
    print(f"Kernel registered: {KERNEL_DISPLAY} ({KERNEL_NAME})")
    print()
    print("In Cursor / VS Code:")
    print("  1. Open folder: d:\\azure  (repo root, not day7 only)")
    print("  2. Open day7/notebooks/test.ipynb")
    print(f"  3. Top-right kernel -> pick: {KERNEL_DISPLAY}")
    print("  4. Run the first code cell")
    print()
    try:
        smoke_execute_test_notebook()
        print("Smoke test: test.ipynb executed successfully via Jupyter.")
    except subprocess.CalledProcessError as exc:
        print(f"Smoke test skipped or failed ({exc}). Select kernel manually in UI.")


if __name__ == "__main__":
    main()
