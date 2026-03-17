# a01_60_engine_controller/run_a01_60_engine_controller_v6.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
a01_60_engine_controller (Ver.6)

※重要:
Windows で
  python a01_60_engine_controller\run_a01_60_engine_controller_v6.py ...
の形式で「スクリプト直実行」すると、プロジェクトルートが sys.path に入らず
`from a01_60_engine_controller...` が失敗することがある。

本ファイルは起動責任として、必ずプロジェクトルートを sys.path 先頭に追加してから
a01_60_engine_controller パッケージを import する。

追加:
- dispatcher_threads / worker_threads を標準出力へ表示
- 起動開始〜終了までの経過秒、pos数、秒/pos を標準出力へ表示
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _count_pos_dirs(workspaces_root: Path) -> int:
    cur = workspaces_root / "current"
    if not cur.exists():
        return 0
    return sum(
        1
        for p in cur.iterdir()
        if p.is_dir()
        and p.name.startswith("pos")
        and len(p.name) == 9
        and p.name[3:].isdigit()
    )


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_a01_60_engine_controller_v6")
    ap.add_argument("--workspaces_root", required=True)
    ap.add_argument("--mode", required=True)
    ap.add_argument("--worker_threads", type=int, default=4)
    ap.add_argument("--dispatcher_mode", choices=["legacy", "dispatch"], default="legacy")

    args = ap.parse_args()
    ws_root = Path(args.workspaces_root)

    # ★プロジェクトルートを sys.path に追加
    pr = _project_root()
    if str(pr) not in sys.path:
        sys.path.insert(0, str(pr))

    # ---- スレッド情報表示 ----
    dispatcher_threads = 1  # 現構造は単一ディスパッチャ
    worker_threads = int(args.worker_threads)

    print("")
    print("=== THREAD CONFIG ===")
    print(f"dispatcher_threads: {dispatcher_threads}")
    print(f"worker_threads:     {worker_threads}")
    print("=====================")

    # ---- 計測開始 ----
    t0 = time.perf_counter()

    from a01_60_engine_controller.v6_dispatcher import run_dispatcher_v6

    rc = run_dispatcher_v6(
        workspaces_root=ws_root,
        mode=str(args.mode),
        worker_threads=worker_threads,
        dispatcher_mode=str(args.dispatcher_mode),
    )

    # ---- 計測終了 ----
    dt = time.perf_counter() - t0
    pos_n = _count_pos_dirs(ws_root)
    per_pos = (dt / pos_n) if pos_n > 0 else 0.0

    print("")
    print("=== PERF (run_a01_60_engine_controller_v6) ===")
    print(f"elapsed_sec: {dt:.6f}")
    print(f"pos_count:   {pos_n}")
    print(f"sec_per_pos: {per_pos:.6f}")
    print("=============================================")

    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())