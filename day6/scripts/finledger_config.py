"""FinLedger run configuration — dataclass used across Day 6+ notebooks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunConfig:
    """One batch run of the FinLedger pipeline."""

    run_date: str
    learner: str
    storage_account: str = ""

    @property
    def bronze_container(self) -> str:
        return "bronze"

    @property
    def bronze_loaded_dir(self) -> Path:
        return Path(f"loaded/run={self.run_date}")

    def bronze_csv_name(self, filename: str = "sample_transactions.csv") -> str:
        return str(self.bronze_loaded_dir / filename)

    def abfss_bronze(self, filename: str = "sample_transactions.csv") -> str:
        if not self.storage_account:
            raise ValueError("storage_account required for abfss paths")
        return (
            f"abfss://{self.bronze_container}@{self.storage_account}.dfs.core.windows.net/"
            f"{self.bronze_csv_name(filename)}"
        )

    def local_data_path(self, filename: str, session_root: Path) -> Path:
        return session_root / "data" / filename
