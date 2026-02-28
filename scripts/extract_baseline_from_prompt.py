#!/usr/bin/env python3
import json
import re
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parents[1]


def extract_first_json(text: str) -> str:
    # crude brace matching to grab the first top-level JSON object
    start = text.find('{')
    if start < 0:
        raise ValueError('no JSON object found')
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    raise ValueError('unterminated JSON object')


def main():
    p = argparse.ArgumentParser(description='Extract PART2 baseline JSON embedded in prompt files to a standalone JSON')
    p.add_argument('--prompt-file', default=str(ROOT / 'prompt' / 'prompt.md'))
    p.add_argument('--out', required=True)
    args = p.parse_args()

    path = Path(args.prompt_file)
    if not path.exists():
        # try final_prompt.md or root final_prompt.md
        for cand in [ROOT / 'prompt' / 'final_prompt.md', ROOT / 'final_prompt.md']:
            if cand.exists():
                path = cand
                break
    text = path.read_text(encoding='utf-8', errors='ignore')
    json_str = extract_first_json(text)
    # validate JSON
    data = json.loads(json_str)
    Path(args.out).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Wrote baseline JSON to {args.out}')


if __name__ == '__main__':
    main()

