#!/usr/bin/env python3
"""
Remove trailing commas at the end of each line for all .csv files
under a given directory (non-recursive by default).

Usage:
  python scripts/fix_trailing_commas.py \
      --dir spider_data/spider_data/database_mysql/customer_deliveries \
      [--recursive] [--no-backup]

Behavior:
  - For each .csv file, removes a single trailing comma (and any spaces
    immediately before it) at end-of-line without touching commas inside fields.
  - Preserves original line endings (\n/\r\n) and only overwrites files that change.
  - Creates a .bak backup before overwriting unless --no-backup is passed.
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path


TRAILING_COMMA_PATTERN = re.compile(r"[ \t]*,$")


def fix_file(path: Path) -> tuple[int, int, bool, str]:
    """Fix a single CSV file.

    Returns (lines_total, lines_modified, changed, new_data)
    """
    try:
        data = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Fallback to latin-1 if encoding is not UTF-8
        data = path.read_text(encoding="latin-1")

    lines = data.splitlines(keepends=True)
    total = len(lines)
    modified = 0
    new_lines = []
    for line in lines:
        # Separate content from the end-of-line characters
        content = line.rstrip("\r\n")
        eol = line[len(content):]
        new_content = TRAILING_COMMA_PATTERN.sub("", content)
        if new_content != content:
            modified += 1
        new_lines.append(new_content + eol)

    changed = (new_lines != lines)
    new_data = "".join(new_lines)
    return total, modified, changed, new_data


def process_dir(directory: Path, recursive: bool, backup: bool) -> None:
    if not directory.exists() or not directory.is_dir():
        raise SystemExit(f"Directory not found: {directory}")

    pattern = "**/*.csv" if recursive else "*.csv"
    csv_files = sorted(directory.glob(pattern))

    if not csv_files:
        print(f"No CSV files found under {directory}")
        return

    total_files = 0
    changed_files = 0
    total_lines = 0
    total_modified_lines = 0

    for csv_path in csv_files:
        total_files += 1
        total, modified, changed, new_data = fix_file(csv_path)
        total_lines += total
        total_modified_lines += modified
        if changed:
            changed_files += 1
            if backup:
                bak_path = csv_path.with_suffix(csv_path.suffix + ".bak")
                if not bak_path.exists():
                    shutil.copy2(csv_path, bak_path)
            # Write updated content
            csv_path.write_text(new_data, encoding="utf-8")
            print(f"Fixed: {csv_path} (modified {modified}/{total} lines)")
        else:
            print(f"Skipped (no change): {csv_path}")

    print("\nSummary:")
    print(f"  Files scanned:   {total_files}")
    print(f"  Files changed:   {changed_files}")
    print(f"  Lines scanned:   {total_lines}")
    print(f"  Lines modified:  {total_modified_lines}")


def main():
    default_dir = Path("spider_data/spider_data/database_mysql/customer_deliveries")
    parser = argparse.ArgumentParser(description="Remove trailing commas in CSV lines.")
    parser.add_argument("--dir", type=Path, default=default_dir, help="Target directory containing CSV files")
    parser.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")
    parser.add_argument("--no-backup", action="store_true", help="Do not write .bak backups before overwriting")
    args = parser.parse_args()

    process_dir(args.dir, recursive=args.recursive, backup=not args.no_backup)


if __name__ == "__main__":
    main()

