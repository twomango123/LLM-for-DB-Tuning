#!/usr/bin/env python3
from pathlib import Path
import shutil

BASE = Path('response')
DEST = BASE / 'perf_fb'

def main():
    DEST.mkdir(parents=True, exist_ok=True)
    moved = 0
    for p in BASE.glob('perf_fb_m*_r*.md'):
        target = DEST / p.name
        try:
            shutil.move(str(p), str(target))
            moved += 1
        except Exception:
            pass
    print(f"migrated {moved} files into {DEST}")

if __name__ == '__main__':
    main()

