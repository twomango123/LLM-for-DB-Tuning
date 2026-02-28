from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class Op:
    kind: str
    args: Dict[str, Any]


def parse_ops(text: str) -> List[Op]:
    """
    解析操作序列文本，支持示例格式：
      TableJoin(customers, customer_addresses, customers.customer_id, customer_addresses.customer_id, False): customers_addresses_combined
      VerticalSplit(products, True): high_frequency_products(product_id, product_name, product_price), low_frequency_products(product_id, product_description)
      TableJoin(actual_orders, regular_orders, actual_orders.regular_order_id, regular_orders.regular_order_id, False): combined_orders
      RedundantColumnAdd(addresses.city, customers_addresses_combined.customer_city, ['addresses.address_id=customers_addresses_combined.address_id'])
      RedundantColumnAdd(products.product_price, actual_order_products.redundant_product_price, ['products.product_id=actual_order_products.product_id'])

    返回结构化 Op 列表。
    """
    ops: List[Op] = []
    for raw in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        # 通用：Kind(args...): tail
        m = re.match(r"^(\w+)\((.*)\)\s*(?::\s*(.*))?$", raw)
        if not m:
            continue
        kind = m.group(1)
        body = m.group(2)
        tail = (m.group(3) or '').strip()

        # 拆分逗号参数（保留中括号内文本）
        parts: List[str] = []
        buf = ''
        depth_sq = 0
        for ch in body:
            if ch == '[':
                depth_sq += 1
            elif ch == ']':
                depth_sq = max(depth_sq - 1, 0)
            if ch == ',' and depth_sq == 0:
                parts.append(buf.strip())
                buf = ''
            else:
                buf += ch
        if buf:
            parts.append(buf.strip())

        def strip_id(s: str) -> str:
            s = s.strip()
            if s.startswith('`') and s.endswith('`'):
                return s[1:-1]
            return s

        if kind.lower() == 'tablejoin':
            # 形如：TableJoin(t1, t2, t1.k, t2.k, False): new_table
            if len(parts) < 5:
                continue
            t1 = strip_id(parts[0])
            t2 = strip_id(parts[1])
            k1 = parts[2].split('.')[-1].strip()
            k2 = parts[3].split('.')[-1].strip()
            keep_old = parts[4].lower() in ('true', '1')
            new_table = tail or f"{t1}_{t2}_joined"
            ops.append(Op('TableJoin', {
                't1': t1, 't2': t2,
                'k1': k1, 'k2': k2,
                'keep_old': keep_old,
                'new_table': new_table,
            }))
            continue

        if kind.lower() == 'verticalsplit':
            # VerticalSplit(products, True): 其余 tail 可忽略用于估算（真实 DDL 由 rewrite 模块负责）
            if len(parts) < 2:
                continue
            table = strip_id(parts[0])
            keep_old = parts[1].lower() in ('true', '1')
            ops.append(Op('VerticalSplit', {
                'table': table,
                'keep_old': keep_old,
                'tail': tail,
            }))
            continue

        if kind.lower() == 'redundantcolumnadd':
            # RedundantColumnAdd(src.col, tgt.col, ['src.k=tgt.k'])
            if len(parts) < 3:
                continue
            src = parts[0]
            tgt = parts[1]
            src_table, src_col = src.split('.')[:2]
            tgt_table, tgt_col = tgt.split('.')[:2]
            join_keys_text = parts[2]
            # 提取第一个等值对 a=b
            m2 = re.search(r"([\w.]+)\s*=\s*([\w.]+)", join_keys_text)
            join_pair = None
            if m2:
                a, b = m2.group(1), m2.group(2)
                a_col = a.split('.')[-1]
                b_col = b.split('.')[-1]
                join_pair = (a_col, b_col)
            ops.append(Op('RedundantColumnAdd', {
                'src_table': src_table,
                'src_col': src_col,
                'tgt_table': tgt_table,
                'tgt_col': tgt_col,
                'join_pair': join_pair,
            }))
            continue

    return ops

