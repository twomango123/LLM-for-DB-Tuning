#!/usr/bin/env python3
import subprocess, sys, time, re, os
from pathlib import Path

CMD = [
  sys.executable, 'auto_loop.py',
  '--parallel-m','8','--rounds-n','6','--select-k','3','--opt-rounds-s','3',
  '--schema-sql','output_dir/schema/schema.sql','--eval-sql-dir','output_dir/sql',
  '--eval-mode','static','--no-db-eval','--use-explain-debug','--explain-debug-dir','part2_debug',
  '--eval-db-config','query_latency/db_config.ini','--storage-meta','output_dir/meta.json','--storage-budget','10GB',
  '--clean-out'
]

runs = int(os.environ.get('BATCH_RUNS','1'))
LOGS = Path('response/logs')
PERF = Path('response/perf_fb')

for i in range(1, runs+1):
    print(f"\n=== Run #{i} starting ===")
    t0 = time.time()
    # auto_loop 现已默认清理输出目录，这里不再重复清理
    p = subprocess.run(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    dt = time.time() - t0
    print(f"Run #{i} rc={p.returncode} took {dt:.1f}s")
    if p.returncode != 0:
        print("stdout:\n", p.stdout[-2000:])
        print("stderr:\n", p.stderr[-2000:])
    # summarize latest runner logs and perf feedback
    if LOGS.exists():
        logs = sorted(LOGS.glob('runner_m*_r*.log'), key=lambda p: p.stat().st_mtime)[-10:]
        for lg in logs:
            txt = lg.read_text(errors='ignore')
            if '合法性检查未通过' in txt:
                print(f"[WARN] {lg.name}: 合法性检查未通过")
                for ln in txt.splitlines():
                    if '列不存在' in ln or '表不存在' in ln:
                        print('  -', ln.strip())
    if PERF.exists():
        pfs = sorted(PERF.glob('perf_fb_m*_r*.md'), key=lambda p: p.stat().st_mtime)[-6:]
        for pf in pfs:
            head = ''.join(pf.read_text(errors='ignore').splitlines(True)[:8])
            print(f"[PERF] {pf.name}:\n{head}")
print("\nBatch finished.")
