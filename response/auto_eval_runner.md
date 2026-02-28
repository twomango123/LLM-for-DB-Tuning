# Runner Auto Eval (per-step)

| Plan | Final Total Cost | Note |
|---|---:|---|
| q1.txt | 202736.0 | [TableJoin] 仅单表且 keep_old=True，成本不变；[HorizontalSplit] 未命中分片，增加 UNION ALL 代价≈0 |
| q10.txt | 1978850450033.0 | [TableJoin] 仅使用 addresses，按新表行数/行宽缩放 |
| q11.txt | 203626.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q12.txt | 301044.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q13.txt | 102040.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q14.txt | 202820.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q15.txt | 3.33 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q16.txt | 3340900.998 | [TableJoin] 仅使用 addresses，按新表行数/行宽缩放 |
| q2.txt | 3902064.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q3.txt | 47.5 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q4.txt | 300554.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q5.txt | 202932.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q6.txt | 325614.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q7.txt | 99602607305.99738 | [TableJoin] 移除旧表 JOIN，替换为新表扫描（忽略 keep_old） |
| q8.txt | 200696.0 | [TableJoin] 仅单表且 keep_old=True，成本不变 |
| q9.txt | 2530752.2460000003 | [TableJoin] 移除旧表 JOIN，替换为新表扫描（忽略 keep_old） |