受影响计划覆盖率：4/16（25.0%）；样本：q2.txt, q7.txt, q9.txt, q16.txt
性能评估对比（cost）：
- 初始基线：197912702237 cost
- 预测结果：148108520372 cost（改善 49804181865）
代表性查询净效应：
- q7.txt: 总成本下降 4.98e+10
- q9.txt: 总成本下降 1.21e+06
- q16.txt: 总成本下降 1.2e+06
写入开销估计：
- OP1 TableJoin: INSERT≈2e+06
- OP2 TableJoin: INSERT≈2e+06

逐操作效果（基于 runner per-step 汇总）：
- 第1步 TableJoin: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第2步 TableJoin: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第3步 VerticalSplit: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第4步 VerticalSplit: 单步上升 0，累计 +0；命中改写比≈0.0%
- 第5步 RedundantColumnAdd: 单步上升 0，累计 +0；命中改写比≈0.0%
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