# -*- coding: utf-8 -*-
"""
a01_61_csa_splitter (Ver.6)  ※改訂版

固定仕様（更新）:
- 入力：<workspaces_root>/root_state.json（CSA_RECORD）
- 出力：<workspace(pos_dir)>/
  - root_state_61.json ：CSA_RECORD保持（改変禁止）
  - root_state_csa.json：turn_sign/nonturn_sign（+/- のみ）
- a01_61 は「分離のみ」。指示書配置も work_order 作成も行わない。
  （指示書はディスパッチャが pos に入れる。work_order もディスパッチャが作る。）

注意：
- workspace(pos_dir) が存在しない場合はエラー（a01_60 が作る）
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_csa_record(obj: Dict[str, Any]) -> None:
    if obj.get("format") != "CSA_RECORD":
        raise ValueError("workspaces/root_state.json must be CSA_RECORD (format=CSA_RECORD)")
    raw = obj.get("raw_lines")
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise ValueError("workspaces/root_state.json must have raw_lines: list[str]")


def detect_turn_sign(raw_lines: List[str]) -> str:
    """
    CSAの手番規定（単独行の '+' or '-'）を検出。
    """
    for ln in raw_lines:
        s = ln.strip()
        if s == "+":
            return "+"
        if s == "-":
            return "-"
    raise ValueError("CSA turn sign line ('+' or '-') not found")


def run_a01_61(workspaces_root: Path, position_dir: Path) -> None:
    root_state_path = workspaces_root / "root_state.json"
    if not root_state_path.exists():
        raise FileNotFoundError(f"missing dev input: {root_state_path}")

    if not position_dir.exists():
        raise FileNotFoundError(f"position_dir not found (a01_60 must create): {position_dir}")

    src = read_json(root_state_path)
    ensure_csa_record(src)

    raw_lines: List[str] = src["raw_lines"]
    turn_sign = detect_turn_sign(raw_lines)
    nonturn_sign = "-" if turn_sign == "+" else "+"

    # root_state_61.json（CSA_RECORD保持）
    write_json(
        position_dir / "root_state_61.json",
        {
            "schema": "root_state_61.v6",
            "csa_record": {
                "format": "CSA_RECORD",
                "version": src.get("version", "csa00_v1"),
                "source_name": src.get("source_name", root_state_path.name),
                "raw_lines": raw_lines,  # 改変しない
            },
        },
    )

    # root_state_csa.json（+/-のみ）
    write_json(
        position_dir / "root_state_csa.json",
        {
            "schema": "root_state_csa.v6",
            "turn_sign": turn_sign,
            "nonturn_sign": nonturn_sign,
        },
    )


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="a01_61_csa_splitter_v6")
    ap.add_argument("--workspaces_root", required=True)
    ap.add_argument("--workspace", required=True)  # position_dir
    args = ap.parse_args(argv)

    run_a01_61(Path(args.workspaces_root), Path(args.workspace))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
