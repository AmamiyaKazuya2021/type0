# a01_60_engine_controller/v6_dispatcher.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import OrderedDict
import json
import os
import time
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple

import shutil

from a01_64_create_tesuji_files.run_a01_64_v6 import build_state_key

from .v6_types import WorkItem, WorkResult
from .v6_worker import start_worker_pool
from a01_71_clear_workspace.clear_workspace_v6 import clear_workspace_v6
from a01_70_instructions.instruction_manager_v6 import copy_instruction_to_position

JST = timezone(timedelta(hours=9))

INSTRUCTION_RANK_MIN = 1
INSTRUCTION_RANK_MAX = 20

_E08_20_OUT = "e08_20_delete_rule_judge.v1.json"

_MAX_PROCESS_POS_NO = 500


def now_iso() -> str:
    return datetime.now(JST).isoformat(timespec="seconds")


def _log_dispatch_task(event: str, task_id: str, instruction_rank: int, position_dir: Path, ok: Optional[bool] = None, message: str = "", quiet: bool = False) -> None:
    if quiet:
        return
    if event == "START":
        print(f"[{now_iso()}] [TASK START] task_id={task_id} rank={int(instruction_rank)} pos={position_dir.name}")
        return
    if ok is None:
        print(f"[{now_iso()}] [TASK DONE] task_id={task_id} rank={int(instruction_rank)} pos={position_dir.name}")
        return
    st = "ok" if ok else "failed"
    if message:
        print(f"[{now_iso()}] [TASK DONE] task_id={task_id} rank={int(instruction_rank)} pos={position_dir.name} status={st} message={message}")
    else:
        print(f"[{now_iso()}] [TASK DONE] task_id={task_id} rank={int(instruction_rank)} pos={position_dir.name} status={st}")


def _dispatcher_lock_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "work_board.lock"


def _acquire_dispatcher_lock(workspaces_root: Path) -> Path:
    lock_path = _dispatcher_lock_path(workspaces_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(f"work_board lock exists: {lock_path}")
    try:
        payload = json.dumps({"pid": os.getpid(), "at": now_iso()}, ensure_ascii=False)
        os.write(fd, payload.encode("utf-8"))
    finally:
        os.close(fd)
    return lock_path


def _release_dispatcher_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    for i in range(50):
        try:
            tmp.replace(path)
            break
        except PermissionError:
            if i == 49:
                raise
            time.sleep(0.05 * (i + 1))


def read_json(path: Path) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for i in range(8):
        try:
            txt = path.read_text(encoding="utf-8")
            if not txt.strip():
                last_exc = json.JSONDecodeError("Empty JSON", txt, 0)
            else:
                return json.loads(txt)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            last_exc = e
        time.sleep(0.03 * (i + 1))
    if last_exc is not None:
        raise last_exc
    raise FileNotFoundError(str(path))


def _read_json_ordered(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8")
    return json.loads(txt, object_pairs_hook=OrderedDict)


def _write_json_ordered(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    for i in range(50):
        try:
            tmp.replace(path)
            break
        except PermissionError:
            if i == 49:
                raise
            time.sleep(0.05 * (i + 1))


def work_board_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "work_board.json"


def ensure_work_board(workspaces_root: Path) -> None:
    p = work_board_path(workspaces_root)
    if p.exists():
        return
    write_json(p, {"schema": "gravityfield.v6.work_board", "created_at": now_iso(), "tasks": []})


def dispatch_cursor_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "dispatch_cursor.json"


def ensure_dispatch_cursor(workspaces_root: Path) -> None:
    p = dispatch_cursor_path(workspaces_root)
    if p.exists():
        return
    write_json(
        p,
        {
            "schema": "dispatch_cursor.v1",
            "allocated_last_pos_no": 1,
            "dedup_checked_last_pos_no": 1,
        },
    )


def _e50_script_path() -> Optional[Path]:
    base = Path(__file__).resolve()
    candidates = [
        base.parents[1] / "e50_00_search_tree.v1.engine_sync.py",
        base.parents[1] / "e50_00_search_tree.v1.py",
        base.parents[1] / "a01_50_search_tree" / "e50_00_search_tree.v1.engine_sync.py",
        base.parents[1] / "a01_50_search_tree" / "e50_00_search_tree.v1.py",
        base.parents[1] / "tools" / "e50_00_search_tree.v1.engine_sync.py",
        base.parents[1] / "tools" / "e50_00_search_tree.v1.py",
        base.parents[1] / "tools" / "e50_00_search_tree" / "e50_00_search_tree.v1.engine_sync.py",
        base.parents[1] / "tools" / "e50_00_search_tree" / "e50_00_search_tree.v1.py",
    ]
    for c in candidates:
        if c.is_file():
            return c
    return None


def _e50_ensure_tree(workspaces_root: Path) -> None:
    sp = _e50_script_path()
    if sp is None:
        return
    try:
        subprocess.run(
            [
                sys.executable,
                str(sp),
                "--workspaces_root",
                str(workspaces_root),
                "ensure-tree",
                "--root-pos-id",
                "pos000001",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return


def _e50_add_new_pos(workspaces_root: Path, to_pos_id: str) -> None:
    sp = _e50_script_path()
    if sp is None:
        return
    try:
        subprocess.run(
            [
                sys.executable,
                str(sp),
                "--workspaces_root",
                str(workspaces_root),
                "add-new-pos",
                "--to-pos-id",
                str(to_pos_id),
                "--default-root-pos-id",
                "pos000001",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return


def _e50_aggregate(workspaces_root: Path) -> bool:
    sp = _e50_script_path()
    if sp is None:
        return False
    try:
        cp = subprocess.run(
            [
                sys.executable,
                str(sp),
                "--workspaces_root",
                str(workspaces_root),
                "aggregate",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return cp.returncode == 0
    except Exception:
        return False


def _project_root_path() -> Path:
    return Path(__file__).resolve().parents[1]


def _find_postprocess_script(script_dir_name: str, script_file_name: str) -> Path:
    project_root = _project_root_path()
    candidates = [
        project_root / script_dir_name / script_file_name,
        project_root / script_file_name,
    ]
    for c in candidates:
        if c.is_file():
            return c
    raise FileNotFoundError(str(candidates[0]))


def _run_postprocess_script(script_dir_name: str, script_file_name: str, quiet: bool = False) -> None:
    script_path = _find_postprocess_script(script_dir_name, script_file_name)
    kwargs: Dict[str, Any] = {
        "check": False,
        "cwd": str(_project_root_path()),
    }
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    cp = subprocess.run(
        [sys.executable, str(script_path)],
        **kwargs,
    )
    if cp.returncode != 0:
        raise RuntimeError(f"postprocess script failed: {script_path}")


def _write_a01_81_send(workspaces_root: Path, quiet: bool = False) -> None:
    _run_postprocess_script("a01_81_send", "a01_81_send.py", quiet=quiet)


def _write_a01_82_sq_rotator(workspaces_root: Path, quiet: bool = False) -> None:
    _run_postprocess_script("a01_82_sq_rotator", "a01_82_sq_rotator.v1.py", quiet=quiet)


def _write_a01_83_csa_move_builder(workspaces_root: Path, quiet: bool = False) -> None:
    _run_postprocess_script("a01_83_csa_move_builder", "a01_83_csa_move_builder.v1.py", quiet=quiet)


def _parse_to_pos_ids_from_e01_07(position_dir: Path) -> List[str]:
    p = position_dir / "step_data" / "e01_07_execute_moves_promotion_split.v1.json"
    if not p.is_file():
        return []
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(obj, dict):
        return []
    if isinstance(obj.get("candidates"), dict):
        candidates = obj.get("candidates")
    else:
        candidates = {}
    out: List[str] = []
    for promo_key in ("as_is", "promote"):
        blocks = candidates.get(promo_key, [])
        if not isinstance(blocks, list):
            continue
        for blk in blocks:
            if not isinstance(blk, dict):
                continue
            to_pos = blk.get("to_pos", [])
            if not isinstance(to_pos, list):
                continue
            for pid in to_pos:
                if isinstance(pid, str) and _is_pos_name(pid) and pid != "pos000001":
                    try:
                        if int(pid[3:]) <= _MAX_PROCESS_POS_NO:
                            out.append(pid)
                    except Exception:
                        pass
    return sorted(set(out))


def board_add_task(workspaces_root: Path, task: Dict[str, Any]) -> None:
    p = work_board_path(workspaces_root)
    board = read_json(p) if p.exists() else {"schema": "gravityfield.v6.work_board", "tasks": []}
    board.setdefault("tasks", []).append(task)
    write_json(p, board)


def board_mark(workspaces_root: Path, task_id: str, status: str, result: Optional[Dict[str, Any]] = None) -> None:
    p = work_board_path(workspaces_root)
    board = read_json(p)
    tasks = board.get("tasks", [])
    for t in tasks:
        if t.get("task_id") == task_id:
            t["status"] = status
            t.setdefault("timestamps", {})[status] = now_iso()
            if result is not None:
                t["result"] = result
            write_json(p, board)
            return
    raise KeyError(task_id)


def board_remove_task(workspaces_root: Path, task_id: str) -> None:
    p = work_board_path(workspaces_root)
    board = read_json(p)
    tasks = board.get("tasks", [])
    board["tasks"] = [t for t in tasks if not (isinstance(t, dict) and t.get("task_id") == task_id)]
    write_json(p, board)


def _is_pos_name(s: str) -> bool:
    return isinstance(s, str) and s.startswith("pos") and len(s) == 9 and s[3:].isdigit()


def _parse_to_pos_ids_from_result_message(msg: str) -> List[str]:
    if not isinstance(msg, str):
        return []
    try:
        obj = json.loads(msg)
    except Exception:
        return []
    if not isinstance(obj, dict):
        return []

    transitions = obj.get("transitions")
    if not isinstance(transitions, list):
        return []

    out: List[str] = []
    for tr in transitions:
        if not isinstance(tr, dict):
            continue
        to_pos_id = tr.get("to_pos_id")
        if isinstance(to_pos_id, str) and _is_pos_name(to_pos_id) and to_pos_id != "pos000001":
            try:
                if int(to_pos_id[3:]) <= _MAX_PROCESS_POS_NO:
                    out.append(to_pos_id)
            except Exception:
                pass

    out = sorted(set(out))
    return out


def _remove_pos_dir_no_residue(pos_dir: Path) -> None:
    if not isinstance(pos_dir, Path):
        pos_dir = Path(pos_dir)

    for i in range(6):
        try:
            shutil.rmtree(pos_dir)
            return
        except Exception:
            time.sleep(0.1 * (i + 1))

    try:
        trash_root = pos_dir.parent / "_deleted_pos"
        trash_root.mkdir(parents=True, exist_ok=True)
        moved = trash_root / f"{pos_dir.name}_{int(time.time())}"
        try:
            pos_dir.replace(moved)
        except Exception:
            shutil.move(str(pos_dir), str(moved))
    except Exception:
        return

    for i in range(6):
        try:
            shutil.rmtree(moved)
            return
        except Exception:
            time.sleep(0.1 * (i + 1))


def holds_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "dispatcher_holds.json"


def read_holds(workspaces_root: Path) -> Dict[str, Any]:
    p = holds_path(workspaces_root)
    if not p.exists():
        return {"schema": "gravityfield.v6.dispatcher_holds", "holds": {}}
    try:
        d = read_json(p)
    except Exception:
        return {"schema": "gravityfield.v6.dispatcher_holds", "holds": {}}
    if not isinstance(d, dict):
        return {"schema": "gravityfield.v6.dispatcher_holds", "holds": {}}
    d.setdefault("schema", "gravityfield.v6.dispatcher_holds")
    d.setdefault("holds", {})
    if not isinstance(d.get("holds"), dict):
        d["holds"] = {}
    return d


def write_holds(workspaces_root: Path, d: Dict[str, Any]) -> None:
    out = dict(d)
    out.setdefault("schema", "gravityfield.v6.dispatcher_holds")
    out.setdefault("holds", {})
    write_json(holds_path(workspaces_root), out)


def set_hold(workspaces_root: Path, pos_name: str, reason: str, position_dir: Path) -> None:
    d = read_holds(workspaces_root)
    holds = d.setdefault("holds", {})
    holds[pos_name] = {"reason": reason, "position_dir": str(position_dir), "held_at": now_iso()}
    write_holds(workspaces_root, d)


def clear_hold(workspaces_root: Path, pos_name: str) -> None:
    d = read_holds(workspaces_root)
    holds = d.get("holds", {})
    if isinstance(holds, dict):
        holds.pop(pos_name, None)
    write_holds(workspaces_root, d)


def _e50_eval_done_report_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "e50_00_update_eval_done.json"


def _consume_e50_eval_done_report(workspaces_root: Path) -> Optional[str]:
    p = _e50_eval_done_report_path(workspaces_root)
    if not p.exists():
        return None
    try:
        obj = read_json(p)
    except Exception:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass
        return None
    pos_id = None
    if isinstance(obj, dict):
        v = obj.get("pos_id")
        if isinstance(v, str) and v:
            pos_id = v
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass
    return pos_id


def _has_active_instruction_rank(board: Dict[str, Any], instruction_rank: int) -> bool:
    tasks = board.get("tasks", [])
    if not isinstance(tasks, list):
        return False
    for t in tasks:
        if not isinstance(t, dict):
            continue
        st = t.get("status")
        if st not in ("queued", "running"):
            continue
        art = t.get("artifacts", {})
        if not isinstance(art, dict):
            continue
        try:
            r = int(art.get("instruction_rank", 0) or 0)
        except Exception:
            r = 0
        if r == int(instruction_rank):
            return True
    return False


def _dispatch_next_instruction18_from_holds_if_possible(workspaces_root: Path, board: Dict[str, Any]) -> None:
    if _has_active_instruction_rank(board, 18):
        return

    holds_obj = read_holds(workspaces_root)
    holds = holds_obj.get("holds", {})
    if not isinstance(holds, dict):
        return

    keys = sorted(list(holds.keys()))
    for k in keys:
        if k == "pos000001":
            continue
        hold_item = holds.get(k, {})
        pos_path_str = hold_item.get("position_dir") if isinstance(hold_item, dict) else None
        pos_path = (
            Path(pos_path_str)
            if isinstance(pos_path_str, str) and pos_path_str
            else (workspaces_root / "current" / k)
        )
        if not _task_exists(board, pos_path, 18):
            next_id = f"{_max_task_id(board) + 1:06d}"
            _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=18, position_dir=pos_path)
        clear_hold(workspaces_root, k)
        return


def _board_has_inflight(board: Dict[str, Any]) -> bool:
    tasks = board.get("tasks", [])
    if not isinstance(tasks, list):
        return False
    for t in tasks:
        if not isinstance(t, dict):
            continue
        st = t.get("status")
        if st in ("queued", "running"):
            return True
    return False


def _dispatch_instruction19_if_possible(workspaces_root: Path, board: Dict[str, Any]) -> None:
    if _board_has_inflight(board):
        return
    if _has_active_instruction_rank(board, 19):
        return
    pos_path = workspaces_root / "current" / "pos000001"
    if _task_exists(board, pos_path, 19):
        return
    next_id = f"{_max_task_id(board) + 1:06d}"
    _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=19, position_dir=pos_path)


def _max_task_id(board: Dict[str, Any]) -> int:
    m = 0
    tasks = board.get("tasks", [])
    if isinstance(tasks, list):
        for t in tasks:
            if isinstance(t, dict):
                tid = t.get("task_id")
                try:
                    v = int(str(tid))
                    if v > m:
                        m = v
                except Exception:
                    pass
    return m


def _alloc_next_task_id(workspaces_root: Path) -> str:
    board = read_json(work_board_path(workspaces_root))
    return f"{_max_task_id(board) + 1:06d}"


def _task_exists(board: Dict[str, Any], position_dir: Path, instruction_rank: int) -> bool:
    tasks = board.get("tasks", [])
    if not isinstance(tasks, list):
        return False
    for t in tasks:
        if not isinstance(t, dict):
            continue
        st = t.get("status")
        if st not in ("queued", "running", "done"):
            continue
        art = t.get("artifacts", {})
        if not isinstance(art, dict):
            continue
        if str(art.get("position_dir", "")) == str(position_dir):
            try:
                r = int(art.get("instruction_rank", 0) or 0)
            except Exception:
                r = 0
            if r == instruction_rank:
                return True
    return False


def _all_true_first_ply(obj: Any) -> bool:
    bools = []

    def collect(x: Any) -> None:
        if isinstance(x, bool):
            bools.append(x)
        elif isinstance(x, dict):
            if "moves" in x:
                collect(x["moves"])
                return
            if "items" in x:
                collect(x["items"])
                return
            for v in x.values():
                if isinstance(v, (bool, dict, list)):
                    collect(v)
        elif isinstance(x, list):
            for v in x:
                if isinstance(v, (bool, dict, list)):
                    collect(v)

    collect(obj)
    return len(bools) > 0 and all(bools)


def _read_first_ply_priority_set(workspaces_root: Path) -> Optional[Any]:
    p = workspaces_root / "current" / "first_ply_priority_set.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def pos_index_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "pos_index.json"


def _load_pos_index(workspaces_root: Path) -> Dict[str, Any]:
    p = pos_index_path(workspaces_root)
    if not p.exists():
        return {"items": []}
    try:
        obj = read_json(p)
    except Exception:
        return {"items": []}
    if not isinstance(obj, dict):
        return {"items": []}
    items = obj.get("items", [])
    if not isinstance(items, list):
        items = []
    clean: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        pn = it.get("pos_name")
        sk = it.get("state_key")
        if isinstance(pn, str) and pn and isinstance(sk, str) and sk:
            clean.append({"pos_name": pn, "state_key": sk})
    return {"items": clean}


def _save_pos_index(workspaces_root: Path, pos_index: Dict[str, Any]) -> None:
    write_json(pos_index_path(workspaces_root), {"items": pos_index.get("items", [])})


def _compute_state_key_from_pos_dir(pos_dir: Path) -> str:
    rs_path = pos_dir / "root_state_00.json"
    root = json.loads(rs_path.read_text(encoding="utf-8"))
    if not isinstance(root, dict):
        raise ValueError(f"root_state_00.json must be object: {rs_path}")
    return build_state_key(root)


def _register_new_positions_from_hint(workspaces_root: Path, pos_names: List[str]) -> None:
    if not pos_names:
        return

    cur = workspaces_root / "current"
    if not cur.exists():
        return

    uniq: list[str] = []
    seen = set()
    for n in pos_names:
        if not _is_pos_name(n):
            continue
        try:
            if int(n[3:]) > _MAX_PROCESS_POS_NO:
                continue
        except Exception:
            continue
        if n == "pos000001":
            continue
        if n in seen:
            continue
        seen.add(n)
        uniq.append(n)
    if not uniq:
        return

    pos_index = _load_pos_index(workspaces_root)
    items = pos_index.get("items", [])
    if not isinstance(items, list):
        items = []
        pos_index["items"] = items

    known_pos = {it.get("pos_name") for it in items if isinstance(it, dict)}
    known_state = {it.get("state_key") for it in items if isinstance(it, dict)}

    board = read_json(work_board_path(workspaces_root))

    for name in uniq:
        pd = cur / name
        if not pd.is_dir():
            continue
        if name in known_pos:
            continue
        if not (pd / "root_state_00.json").is_file():
            continue

        try:
            sk = _compute_state_key_from_pos_dir(pd)
        except Exception:
            continue

        if sk in known_state:
            try:
                _remove_pos_dir_no_residue(pd)
            except Exception:
                pass
            continue

        items.append({"pos_name": pd.name, "state_key": sk})
        known_pos.add(pd.name)
        known_state.add(sk)

        if not _task_exists(board, pd, 2):
            next_id = f"{_max_task_id(board) + 1:06d}"
            _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=2, position_dir=pd)
            board = read_json(work_board_path(workspaces_root))

    _save_pos_index(workspaces_root, pos_index)


def _pick_next_task(board: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tasks = board.get("tasks", [])
    if not isinstance(tasks, list):
        return None

    best: Optional[Tuple[int, int, Dict[str, Any]]] = None
    for idx, t in enumerate(tasks):
        if not isinstance(t, dict):
            continue
        if t.get("status") != "queued":
            continue

        artifacts = t.get("artifacts", {})
        rank = INSTRUCTION_RANK_MIN
        if isinstance(artifacts, dict):
            try:
                rank = int(artifacts.get("instruction_rank", INSTRUCTION_RANK_MIN))
            except Exception:
                rank = INSTRUCTION_RANK_MIN
        rank = max(INSTRUCTION_RANK_MIN, min(INSTRUCTION_RANK_MAX, rank))

        if best is None or rank > best[0] or (rank == best[0] and idx < best[1]):
            best = (rank, idx, t)

    return None if best is None else best[2]


def _write_work_order_instruction_01(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "a01_61_csa_splitter", "version": "v6"},
            {"id": "a01_62_csa_rotator", "version": "v6"},
            {"id": "a01_63_csa_normalizer", "version": "v6"},
            {"id": "a01_64", "version": "v6"},
        ],
        "planned_steps": ["m07_00_state", "m07_01a", "m07_01b", "m07_01c", "m07_02_nojump", "m07_03_legal_moves"],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_02(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "m07_00_state", "version": "v6"},
            {"id": "m07_01a", "version": "v6"},
            {"id": "m07_01b", "version": "v6"},
            {"id": "m07_01c", "version": "v6"},
            {"id": "m07_02_nojump", "version": "v6"},
            {"id": "m07_03_legal_moves", "version": "v6"},
        ],
        "planned_steps": ["m07_04a", "m07_04b", "m07_04c", "m07_04d", "m07_05_uchi_fu_zume", "m07_06"],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_03(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "m07_04a", "version": "v6"},
            {"id": "m07_04b", "version": "v6"},
            {"id": "m07_04c", "version": "v6"},
            {"id": "m07_04d", "version": "v6"},
            {"id": "m07_05_uchi_fu_zume", "version": "v6"},
            {"id": "m07_06", "version": "v6"},
        ],
        "planned_steps": ["m10_00a", "m10_00c", "m10_00d", "m10_00e", "m10_00f_kiki"],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_04(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "m10_00a", "version": "v6"},
            {"id": "m10_00c", "version": "v6"},
            {"id": "m10_00d", "version": "v6"},
            {"id": "m10_00e", "version": "v6"},
            {"id": "m10_00f_kiki", "version": "v6"},
        ],
        "planned_steps": ["m12_01a", "m12_90_base_flags"],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_05(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "m12_01a", "version": "v6"},
            {"id": "m12_90_base_flags", "version": "v6"},
        ],
        "planned_steps": ["e08_executor"],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_06(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [{"id": "e08_executor", "version": "v6"}],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_07(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "e01_03_execute_moves_confirmer", "version": "v6"},
            {"id": "e01_04_execute_moves_legalfilter", "version": "v6"},
            {"id": "e01_05_execute_moves_selector", "version": "v6"},
        ],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_09(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [{"id": "e11_02_reading_continuation_decision_engine", "version": "v6"}],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_10(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "e01_03_execute_moves_confirmer", "version": "v6"},
            {"id": "e01_04_execute_moves_legalfilter", "version": "v6"},
            {"id": "e01_05_execute_moves_selector", "version": "v6"},
            {"id": "e01_06_execute_moves_promotion_split", "version": "v6"},
        ],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_11(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "e03_00_board_rotation_executor", "version": "v6"},
            {"id": "e03_01_rotation_state_builder", "version": "v6"},
            {"id": "e03_02_first_ply_priority_set_builder", "version": "v6"},
        ],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_18(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [{"id": "e55_02_board_eval_engine", "version": "v6"}],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_19(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [{"id": "e50_00_search_tree_aggregate", "version": "v6"}],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _write_work_order_instruction_20(position_dir: Path) -> None:
    wo = {
        "schema": "gravityfield.v6.work_order",
        "steps": [
            {"id": "a01_81_send", "version": "v6"},
            {"id": "a01_82_sq_rotator", "version": "v6"},
            {"id": "a01_83_csa_move_builder", "version": "v6"},
        ],
        "planned_steps": [],
    }
    write_json(position_dir / "work_order.json", wo)


def _update_search_tree_eval_from_e55_02(workspaces_root: Path, pos_name: str, e55_02_path: Path) -> None:
    if not e55_02_path.is_file():
        raise FileNotFoundError(str(e55_02_path))

    e55 = read_json(e55_02_path)
    if not isinstance(e55, dict):
        raise ValueError(str(e55_02_path))

    eval_map: Dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0, "G": 0}
    eval_done_map: Dict[str, bool] = {"A": False, "B": False, "C": False, "D": False, "E": False, "F": False, "G": False}

    for g in ("A", "B", "C", "D", "E", "F", "G"):
        v = e55.get(g)
        if isinstance(v, dict) and "total_score" in v:
            try:
                eval_map[g] = int(v.get("total_score", 0) or 0)
            except Exception:
                eval_map[g] = 0
            eval_done_map[g] = True

    st_path = workspaces_root / "current" / "search_tree.json"
    if not st_path.exists():
        raise FileNotFoundError(str(st_path))

    tree = _read_json_ordered(st_path)

    def walk(node: Any) -> bool:
        if isinstance(node, dict):
            if node.get("from_pos_id") == pos_name or node.get("to_pos_id") == pos_name:
                ev = node.get("eval")
                ed = node.get("eval_done")
                if not isinstance(ev, dict) or not isinstance(ed, dict):
                    raise ValueError(pos_name)

                for k in ("A", "B", "C", "D", "E", "F", "G"):
                    if k in ev:
                        ev[k] = int(eval_map.get(k, 0))
                    if k in ed:
                        ed[k] = bool(eval_done_map.get(k, False))
                return True

            root_node = node.get("root")
            if isinstance(root_node, (dict, list)):
                if walk(root_node):
                    return True

            children = node.get("children")
            if isinstance(children, list):
                for c in children:
                    if walk(c):
                        return True
        elif isinstance(node, list):
            for c in node:
                if walk(c):
                    return True
        return False

    if not walk(tree):
        raise KeyError(pos_name)

    _write_json_ordered(st_path, tree)


def _create_position_dir(workspaces_root: Path, task_id: str) -> Path:
    current = workspaces_root / "current"
    current.mkdir(parents=True, exist_ok=True)

    pos_dir = current / f"pos{task_id}"
    pos_dir.mkdir(parents=True, exist_ok=True)

    (pos_dir / "step_data").mkdir(parents=True, exist_ok=True)
    return pos_dir


def _enqueue_instruction(
    workspaces_root: Path,
    task_id: str,
    instruction_rank: int,
    position_dir: Optional[Path] = None,
) -> Path:
    if position_dir is None:
        pos_dir = _create_position_dir(workspaces_root, task_id)
    else:
        pos_dir = Path(position_dir)
        (pos_dir / "step_data").mkdir(parents=True, exist_ok=True)

    try:
        copy_instruction_to_position(workspaces_root=workspaces_root, position_dir=pos_dir, rank=instruction_rank)
    except FileNotFoundError:
        pass

    if instruction_rank == 1:
        _write_work_order_instruction_01(pos_dir)
    elif instruction_rank == 2:
        _write_work_order_instruction_02(pos_dir)
    elif instruction_rank == 3:
        _write_work_order_instruction_03(pos_dir)
    elif instruction_rank == 4:
        _write_work_order_instruction_04(pos_dir)
    elif instruction_rank == 5:
        _write_work_order_instruction_05(pos_dir)
    elif instruction_rank == 6:
        _write_work_order_instruction_06(pos_dir)
    elif instruction_rank == 7:
        _write_work_order_instruction_07(pos_dir)
    elif instruction_rank == 9:
        _write_work_order_instruction_09(pos_dir)
    elif instruction_rank == 10:
        _write_work_order_instruction_10(pos_dir)
    elif instruction_rank == 11:
        _write_work_order_instruction_11(pos_dir)
    elif instruction_rank == 18:
        _write_work_order_instruction_18(pos_dir)
    elif instruction_rank == 19:
        _write_work_order_instruction_19(pos_dir)
    elif instruction_rank == 20:
        _write_work_order_instruction_20(pos_dir)
    else:
        raise ValueError(f"unsupported instruction_rank: {instruction_rank}")

    board_add_task(
        workspaces_root,
        {
            "task_id": task_id,
            "status": "queued",
            "artifacts": {"instruction_rank": instruction_rank, "position_dir": str(pos_dir)},
            "created_at": now_iso(),
        },
    )

    return pos_dir


def _read_e11_02_decision(position_dir: Path) -> bool:
    p = position_dir / "step_data" / "e11_02_reading_continuation_decision_engine.v1.json"
    d = read_json(p)
    return bool(d.get("decision", False))


def _read_e08_overall_delete_judge(position_dir: Path) -> bool:
    p = position_dir / "step_data" / _E08_20_OUT
    d = read_json(p)
    return bool(d.get("overall_delete_judge", False))


def run_dispatcher_v6(workspaces_root: Path, mode: str, worker_threads: int = 2, dispatcher_mode: str = "legacy") -> int:
    quiet = str(mode).lower() == "usi"
    clear_workspace_v6(workspaces_root=workspaces_root, dry_run=False)

    ensure_work_board(workspaces_root)
    ensure_dispatch_cursor(workspaces_root)
    _e50_ensure_tree(workspaces_root)
    lock_path = _acquire_dispatcher_lock(workspaces_root)

    try:
        work_q: "Queue[Optional[WorkItem]]" = Queue()
        result_q: "Queue[WorkResult]" = Queue()
        start_worker_pool(worker_threads=int(worker_threads), work_q=work_q, result_q=result_q, quiet=quiet)

        _enqueue_instruction(workspaces_root, task_id="000001", instruction_rank=1, position_dir=None)

        in_flight: Dict[str, Dict[str, Any]] = {}
        aborted = False

        while True:
            board = read_json(work_board_path(workspaces_root))

            if dispatcher_mode == "dispatch":
                done_pos = _consume_e50_eval_done_report(workspaces_root)
                if done_pos is not None:
                    _dispatch_next_instruction18_from_holds_if_possible(workspaces_root, board)
                    board = read_json(work_board_path(workspaces_root))

                holds_obj = read_holds(workspaces_root)
                holds = holds_obj.get("holds", {})
                if isinstance(holds, dict) and "pos000001" in holds:
                    hold_item = holds.get("pos000001", {})
                    pos_path_str = hold_item.get("position_dir") if isinstance(hold_item, dict) else None
                    pos_path = (
                        Path(pos_path_str)
                        if isinstance(pos_path_str, str) and pos_path_str
                        else (workspaces_root / "current" / "pos000001")
                    )

                    fp = _read_first_ply_priority_set(workspaces_root)
                    if fp is not None and _all_true_first_ply(fp):
                        if not _task_exists(board, pos_path, 10):
                            next_id = f"{_max_task_id(board) + 1:06d}"
                            _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=10, position_dir=pos_path)
                        clear_hold(workspaces_root, "pos000001")
                        board = read_json(work_board_path(workspaces_root))

                _dispatch_instruction19_if_possible(workspaces_root, board)
                board = read_json(work_board_path(workspaces_root))

            while (not aborted) and (len(in_flight) < int(worker_threads)):
                task = _pick_next_task(board)
                if task is None:
                    break

                task_id = str(task.get("task_id"))

                artifacts = task.get("artifacts", {})
                if not isinstance(artifacts, dict):
                    artifacts = {}
                try:
                    rank = int(artifacts.get("instruction_rank", 0) or 0)
                except Exception:
                    rank = 0

                if rank == 19:
                    _log_dispatch_task("START", task_id=str(task_id), instruction_rank=19, position_dir=Path(str(artifacts.get("position_dir", ""))), quiet=quiet)
                    board_mark(workspaces_root, task_id, "running")
                    ok = _e50_aggregate(workspaces_root)
                    if ok:
                        board_mark(workspaces_root, task_id, "done", {"message": "e50 aggregate done"})
                        _log_dispatch_task("DONE", task_id=str(task_id), instruction_rank=19, position_dir=Path(str(artifacts.get("position_dir", ""))), ok=True, quiet=quiet)
                        board = read_json(work_board_path(workspaces_root))
                        next_id = f"{_max_task_id(board) + 1:06d}"
                        _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=20, position_dir=Path(str(artifacts.get("position_dir", ""))))
                    else:
                        board_mark(workspaces_root, task_id, "failed", {"message": "e50 aggregate failed"})
                        _log_dispatch_task("DONE", task_id=str(task_id), instruction_rank=19, position_dir=Path(str(artifacts.get("position_dir", ""))), ok=False, message="e50 aggregate failed", quiet=quiet)
                        aborted = True
                    board = read_json(work_board_path(workspaces_root))
                    continue

                if rank == 20:
                    _log_dispatch_task("START", task_id=str(task_id), instruction_rank=20, position_dir=Path(str(artifacts.get("position_dir", ""))), quiet=quiet)
                    board_mark(workspaces_root, task_id, "running")
                    try:
                        _write_a01_81_send(workspaces_root, quiet=quiet)
                        _write_a01_82_sq_rotator(workspaces_root, quiet=quiet)
                        _write_a01_83_csa_move_builder(workspaces_root, quiet=quiet)
                        board_mark(workspaces_root, task_id, "done", {"message": "instruction20 done"})
                        _log_dispatch_task("DONE", task_id=str(task_id), instruction_rank=20, position_dir=Path(str(artifacts.get("position_dir", ""))), ok=True, quiet=quiet)
                    except Exception as e:
                        board_mark(workspaces_root, task_id, "failed", {"message": str(e)})
                        _log_dispatch_task("DONE", task_id=str(task_id), instruction_rank=20, position_dir=Path(str(artifacts.get("position_dir", ""))), ok=False, message=str(e), quiet=quiet)
                        aborted = True
                    board = read_json(work_board_path(workspaces_root))
                    continue

                board_mark(workspaces_root, task_id, "running")
                pos_dir = Path(str(artifacts.get("position_dir", "")))
                _log_dispatch_task("START", task_id=str(task_id), instruction_rank=int(rank), position_dir=pos_dir, quiet=quiet)

                try:
                    work_q.put(WorkItem(task_id=task_id, artifacts=artifacts))
                    in_flight[task_id] = task
                except Exception as e:
                    board_mark(workspaces_root, task_id, "failed", {"message": str(e)})
                    aborted = True
                    break

                board = read_json(work_board_path(workspaces_root))

            if not in_flight:
                break

            res = result_q.get()
            done_task = in_flight.pop(str(res.task_id), None)
            if done_task is None:
                continue

            done_task_id = str(done_task.get("task_id"))

            artifacts = done_task.get("artifacts", {})
            if not isinstance(artifacts, dict):
                artifacts = {}
            try:
                rank_done = int(artifacts.get("instruction_rank", 0) or 0)
            except Exception:
                rank_done = 0
            pos_dir_done = Path(str(artifacts.get("position_dir", "")))

            if res.ok:
                board_mark(workspaces_root, done_task_id, "done", {"message": res.message})
                _log_dispatch_task("DONE", task_id=str(done_task_id), instruction_rank=int(rank_done), position_dir=pos_dir_done, ok=True, quiet=quiet)
            else:
                board_mark(workspaces_root, done_task_id, "failed", {"message": res.message})
                _log_dispatch_task("DONE", task_id=str(done_task_id), instruction_rank=int(rank_done), position_dir=pos_dir_done, ok=False, message=str(res.message), quiet=quiet)
                aborted = True

            if aborted:
                while in_flight:
                    res2 = result_q.get()
                    t2 = in_flight.pop(str(res2.task_id), None)
                    if t2 is None:
                        continue
                    tid2 = str(t2.get("task_id"))
                    if res2.ok:
                        board_mark(workspaces_root, tid2, "done", {"message": res2.message})
                    else:
                        board_mark(workspaces_root, tid2, "failed", {"message": res2.message})
                break

            artifacts = done_task.get("artifacts", {})
            if not isinstance(artifacts, dict):
                artifacts = {}

            try:
                rank = int(artifacts.get("instruction_rank", 0))
            except Exception:
                rank = 0

            pos_dir = Path(str(artifacts.get("position_dir", "")))

            try:
                if res.next_ranks and rank in (1, 2, 3, 4, 5, 6, 9):
                    for next_rank in res.next_ranks:
                        if next_rank == 18 and dispatcher_mode == "dispatch" and _has_active_instruction_rank(read_json(work_board_path(workspaces_root)), 18):
                            set_hold(workspaces_root, pos_dir.name, "wait_instruction18_limit", pos_dir)
                            continue
                        next_id = _alloc_next_task_id(workspaces_root)
                        _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=int(next_rank), position_dir=pos_dir)

                elif rank == 10:
                    e01_07_path = pos_dir / "step_data" / "e01_07_execute_moves_promotion_split.v1.json"

                    allocation_plan = res.allocation_plan if isinstance(res.allocation_plan, dict) else {}
                    candidates = allocation_plan.get("candidates", {})
                    if not isinstance(candidates, dict):
                        candidates = {}

                    cursor_p = dispatch_cursor_path(workspaces_root)
                    cursor = read_json(cursor_p)
                    allocated_last = int(cursor.get("allocated_last_pos_no", 1) or 1)

                    def alloc_pos_id() -> str:
                        nonlocal allocated_last
                        if allocated_last >= _MAX_PROCESS_POS_NO:
                            raise RuntimeError(f"pos limit reached: {_MAX_PROCESS_POS_NO}")
                        allocated_last += 1
                        return f"pos{allocated_last:06d}"

                    out_candidates: Dict[str, Any] = {}
                    for promo_key in ("as_is", "promote"):
                        blocks = candidates.get(promo_key, [])
                        if not isinstance(blocks, list):
                            blocks = []
                        out_blocks: List[Dict[str, Any]] = []
                        for blk in blocks:
                            if not isinstance(blk, dict):
                                continue
                            try:
                                to_pos_count = int(blk.get("to_pos_count", 0) or 0)
                            except Exception:
                                to_pos_count = 0
                            to_pos_list: List[str] = []
                            for _ in range(max(0, to_pos_count)):
                                to_pos_list.append(alloc_pos_id())
                            new_blk = dict(blk)
                            new_blk.pop("to_pos_count", None)
                            new_blk["to_pos"] = to_pos_list
                            out_blocks.append(new_blk)
                        out_candidates[promo_key] = out_blocks

                    e01_07 = {
                        "schema": "e01_07_execute_moves_promotion_split.v1",
                        "from_pos_id": pos_dir.name,
                        "allocated_last_pos_no": allocated_last,
                        "candidates": out_candidates,
                    }
                    write_json(e01_07_path, e01_07)

                    cursor["allocated_last_pos_no"] = allocated_last
                    write_json(cursor_p, cursor)

                    next_rank = 18
                    if res.next_ranks:
                        next_rank = int(res.next_ranks[0])
                    if next_rank == 18 and dispatcher_mode == "dispatch" and _has_active_instruction_rank(read_json(work_board_path(workspaces_root)), 18):
                        set_hold(workspaces_root, pos_dir.name, "wait_instruction18_limit", pos_dir)
                    else:
                        next_id = _alloc_next_task_id(workspaces_root)
                        _enqueue_instruction(workspaces_root, task_id=next_id, instruction_rank=next_rank, position_dir=pos_dir)

                elif rank == 11:
                    pos_names = list(res.new_positions) if isinstance(res.new_positions, list) else []
                    _register_new_positions_from_hint(workspaces_root, pos_names)
                    for pn in pos_names:
                        if (workspaces_root / "current" / pn).is_dir():
                            _e50_add_new_pos(workspaces_root, pn)

                    try:
                        board_latest = read_json(work_board_path(workspaces_root))
                        for t in board_latest.get("tasks", []):
                            if isinstance(t, dict) and str(t.get("task_id")) == done_task_id:
                                if isinstance(t.get("result"), dict):
                                    t["result"]["message"] = ""
                                break
                        write_json(work_board_path(workspaces_root), board_latest)
                    except Exception:
                        pass

                elif rank == 18:
                    report_pos = _consume_e50_eval_done_report(workspaces_root)
                    if report_pos is not None:
                        board_latest = read_json(work_board_path(workspaces_root))
                        _dispatch_next_instruction18_from_holds_if_possible(workspaces_root, board_latest)

            except Exception as e:
                board_mark(workspaces_root, done_task_id, "failed", {"message": str(e)})
                aborted = True
                while in_flight:
                    res2 = result_q.get()
                    t2 = in_flight.pop(str(res2.task_id), None)
                    if t2 is None:
                        continue
                    tid2 = str(t2.get("task_id"))
                    if res2.ok:
                        board_mark(workspaces_root, tid2, "done", {"message": res2.message})
                    else:
                        board_mark(workspaces_root, tid2, "failed", {"message": res2.message})
                break

        for _ in range(int(worker_threads)):
            work_q.put(None)

        return 0
    finally:
        _release_dispatcher_lock(lock_path)