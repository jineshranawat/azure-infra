"""Day 7 — print Python essentials (matches notebook nb_01)."""

from __future__ import annotations

from pathlib import Path


def demo_variables() -> None:
  channels = ["wire", "card", "fps"]
  print("channels:", channels)
  print("first:", channels[0])


def demo_dict() -> None:
  txn = {"transaction_id": "TXN-10003", "amount_gbp": "50000.00"}
  print("id:", txn["transaction_id"])


def demo_function() -> None:
  def clean_amount(value):
    try:
      return float(value)
    except (TypeError, ValueError):
      return 0.0

  print("clean 50000:", clean_amount("50000"))
  print("clean oops:", clean_amount("oops"))


def demo_pathlib(root: Path) -> None:
  csv_path = root / "data" / "sample_transactions.csv"
  print("exists:", csv_path.is_file())
  print("name:", csv_path.name)


def main(root: Path) -> None:
  print("--- Python variables ---")
  demo_variables()
  print("--- Python dict ---")
  demo_dict()
  print("--- Python function ---")
  demo_function()
  print("--- pathlib ---")
  demo_pathlib(root)


if __name__ == "__main__":
  _root = Path(__file__).resolve().parent.parent
  main(_root)
