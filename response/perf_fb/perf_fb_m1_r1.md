受影响计划覆盖率：1/16（6.2%）；样本：q1.txt
性能评估对比（cost）：
- 初始基线：197912702237 cost
- 预测结果：197912499501 cost（改善 202736）
代表性查询净效应：
- q1.txt: 总成本下降 2.03e+05
- q2.txt: 总成本上升 0
- q3.txt: 总成本上升 0

逐操作效果（基于 runner per-step 汇总）：
- 第1步 OP1: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第2步 OP2: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第3步 OP3: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第4步 OP4: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第5步 OP5: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第6步 OP6: 单步上升 0，累计 +0；命中改写比≈0.0%

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