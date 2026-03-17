# a01_60_engine_controller/v6_worker.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional

from .v6_types import WorkItem, WorkResult

JST = timezone(timedelta(hours=9))

_E01_EXECUTE_MOVES_WORKSPACE = "nonboard/editor"


def _now_iso() -> str:
    return datetime.now(JST).isoformat(timespec="seconds")


def _log_step(event: str, position_dir: Path, step_id: str, version: str, elapsed_ms: Optional[int] = None, quiet: bool = False) -> None:
    if quiet:
        return
    if elapsed_ms is None:
        print(f"[{_now_iso()}] [STEP {event}] pos={position_dir.name} step={step_id} ver={version}")
    else:
        print(f"[{_now_iso()}] [STEP {event}] pos={position_dir.name} step={step_id} ver={version} elapsed_ms={int(elapsed_ms)}")


def _log_task(event: str, task_id: str, instruction_rank: Optional[int], position_dir: Path, ok: Optional[bool] = None, message: str = "", quiet: bool = False) -> None:
    if quiet:
        return
    r = "" if instruction_rank is None else str(int(instruction_rank))
    if event == "START":
        print(f"[{_now_iso()}] [TASK START] task_id={task_id} rank={r} pos={position_dir.name}")
        return
    if ok is None:
        print(f"[{_now_iso()}] [TASK DONE] task_id={task_id} rank={r} pos={position_dir.name}")
        return
    st = "ok" if ok else "failed"
    if message:
        print(f"[{_now_iso()}] [TASK DONE] task_id={task_id} rank={r} pos={position_dir.name} status={st} message={message}")
    else:
        print(f"[{_now_iso()}] [TASK DONE] task_id={task_id} rank={r} pos={position_dir.name} status={st}")


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _instruction_measure_dir(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "instruction_measures"


def _instruction_measure_last_path(workspaces_root: Path) -> Path:
    return workspaces_root / "current" / "instruction_measure_last.json"


def _record_instruction_measure(workspaces_root: Path, task_id: str, instruction_rank: int, position_dir: Path, elapsed_ms: int, ok: bool) -> None:
    record = {
        "schema": "instruction_measure.v1",
        "task_id": str(task_id),
        "instruction_rank": int(instruction_rank),
        "position_dir": str(position_dir),
        "position_name": position_dir.name,
        "elapsed_ms": int(elapsed_ms),
        "ok": bool(ok),
        "finished_at": _now_iso(),
    }

    measure_dir = _instruction_measure_dir(workspaces_root)
    measure_dir.mkdir(parents=True, exist_ok=True)

    task_path = measure_dir / f"task_{str(task_id)}.json"
    _write_json_atomic(task_path, record)

    try:
        last_path = _instruction_measure_last_path(workspaces_root)
        _write_json_atomic(last_path, record)
    except Exception:
        pass


def _run_py_module(module: str, args: List[str], quiet: bool = False) -> None:
    cmd = [sys.executable, "-W", "ignore::RuntimeWarning", "-m", module, *args]
    kwargs: Dict[str, Any] = {"check": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    subprocess.run(cmd, **kwargs)


def _run_py_script(script_path: Path, args: List[str], quiet: bool = False) -> None:
    cmd = [sys.executable, "-W", "ignore::RuntimeWarning", str(script_path), *args]
    kwargs: Dict[str, Any] = {"check": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    subprocess.run(cmd, **kwargs)


def _infer_workspaces_root_from_pos(position_dir: Path) -> Path:
    return position_dir.resolve().parents[1]


def _infer_project_root_from_pos(position_dir: Path) -> Path:
    workspaces_root = _infer_workspaces_root_from_pos(position_dir)
    return workspaces_root.resolve().parent


def _list_current_pos_names(workspaces_root: Path) -> List[str]:
    cur = workspaces_root / "current"
    if not cur.exists():
        return []
    out: List[str] = []
    for p in cur.iterdir():
        if (
            p.is_dir()
            and p.name.startswith("pos")
            and len(p.name) == 9
            and p.name[3:].isdigit()
        ):
            out.append(p.name)
    out.sort()
    return out


def _report_eval_to_search_tree(position_dir: Path) -> None:
    project_root = _infer_project_root_from_pos(position_dir)
    workspaces_root = _infer_workspaces_root_from_pos(position_dir)
    script_path = project_root / "tools" / "e50_00_search_tree" / "e50_00_search_tree.v1.engine_sync.py"
    _run_py_script(
        script_path,
        ["--workspaces_root", str(workspaces_root), "update-eval", "--pos-id", str(position_dir.name)],
    )


def _run_step(step_id: str, version: str, position_dir: Path, project_root: Optional[Path] = None, workspaces_root: Optional[Path] = None, quiet: bool = False) -> None:
    if version != "v6":
        raise ValueError(f"unsupported step version: {step_id} {version}")

    if workspaces_root is None:
        workspaces_root = _infer_workspaces_root_from_pos(position_dir)
    if project_root is None:
        project_root = _infer_project_root_from_pos(position_dir)

    if step_id == "a01_61_csa_splitter":
        _run_py_module(
            "a01_61_csa_splitter",
            ["--workspaces_root", str(workspaces_root), "--workspace", str(position_dir)],
            quiet=quiet,
        )
        return

    if step_id == "a01_62_csa_rotator":
        _run_py_module("a01_62_csa_rotator", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "a01_63_csa_normalizer":
        _run_py_module("a01_63_csa_normalizer", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "a01_64":
        _run_py_module(
            "a01_64_create_tesuji_files",
            ["--workspaces_root", str(workspaces_root), "--workspace", str(position_dir)],
            quiet=quiet,
        )
        return

    if step_id == "m07_00_state":
        _run_py_module("m07_00_state.run_m07_00_state_v6", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_01a":
        _run_py_module("m07_01a.run_m07_01a_v6", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_01b":
        _run_py_module("m07_01b.run_m07_01b_v6", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_01c":
        _run_py_module("m07_01c.run_m07_01c_v6", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_02_nojump":
        _run_py_module("m07_02_nojump.run_m07_02_nojump_v6", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_03_legal_moves":
        _run_py_module(
            "m07_03_legal_moves.run_m07_03_legal_moves_v6",
            ["--workspace", str(position_dir)],
            quiet=quiet,
        )
        return

    if step_id == "m07_04a":
        _run_py_module("m07_04a", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_04b":
        _run_py_module("m07_04b", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_04c":
        _run_py_module("m07_04c", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_04d":
        _run_py_module("m07_04d", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_05_uchi_fu_zume":
        _run_py_module("m07_05_uchi_fu_zume", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m07_06":
        _run_py_module("m07_06", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m10_00a":
        _run_py_module("m10_00a", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m10_00c":
        _run_py_module("m10_00c", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m10_00d":
        _run_py_module("m10_00d", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m10_00e":
        _run_py_module("m10_00e", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m10_00f_kiki":
        _run_py_module("m10_00f_kiki", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m12_01a":
        _run_py_module("m12_01a", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "m12_90_base_flags":
        _run_py_module("m12_90_base_flags", ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e08_executor":
        script_path = project_root / "tools" / "e08_executor" / "run_e08_20_delete_rule_judge_v1.py"
        _run_py_script(script_path, [str(position_dir)], quiet=quiet)
        return

    if step_id == "e03_00_board_rotation_executor":
        script_path = (
            project_root
            / "tools"
            / "e03_00_board_rotation_executor"
            / "e03_00_board_rotation_executor.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e03_01_rotation_state_builder":
        script_path = (
            project_root
            / "tools"
            / "e03_01_rotation_state_builder"
            / "e03_01_rotation_state_builder.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e03_02_first_ply_priority_set_builder":
        script_path = (
            project_root
            / "tools"
            / "e03_02_first_ply_priority_set_builder"
            / "e03_02_first_ply_priority_set_builder.py"
        )
        _run_py_script(script_path, [], quiet=quiet)
        return

    if step_id == "e01_02_execute_moves_normalizer":
        script_path = (
            project_root
            / "tools"
            / "e01_02_execute_moves_normalizer"
            / "run_e01_02_execute_moves_normalizer_v1.py"
        )

        in_json = (
            project_root
            / "workspaces"
            / Path(_E01_EXECUTE_MOVES_WORKSPACE)
            / "e01_01_execute_moves.v1.json"
        )

        out_json = position_dir / "step_data" / "e01_01_execute_moves.v1.normalized.json"

        _run_py_script(script_path, ["--in", str(in_json), "--out", str(out_json)], quiet=quiet)
        return

    if step_id == "e01_03_execute_moves_confirmer":
        script_path = (
            project_root
            / "tools"
            / "e01_03_execute_moves_confirmer"
            / "run_e01_03_execute_moves_confirmer_v1.py"
        )
        _run_py_script(script_path, [str(position_dir)], quiet=quiet)
        return

    if step_id == "e01_04_execute_moves_legalfilter":
        script_path = (
            project_root
            / "tools"
            / "e01_04_execute_moves_legalfilter"
            / "e01_04_execute_moves_legalfilter.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e01_05_execute_moves_selector":
        script_path = (
            project_root
            / "tools"
            / "e01_05_execute_moves_selector"
            / "e01_05_execute_moves_selector.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e01_06_execute_moves_promotion_split":
        script_path = (
            project_root
            / "tools"
            / "e01_06_execute_moves_promotion_split"
            / "e01_06_execute_moves_promotion_split.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e01_05_execute_moves_promotion_split":
        script_path = (
            project_root
            / "tools"
            / "e01_06_execute_moves_promotion_split"
            / "e01_06_execute_moves_promotion_split.py"
        )
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e11_02_reading_continuation_decision_engine":
        script_path = (
            project_root
            / "tools"
            / "e11_02_reading_continuation_decision_engine"
            / "run_e11_02_reading_continuation_decision_engine_v1.py"
        )
        _run_py_script(script_path, ["--workspaces_root", str(workspaces_root), "--workspace", str(position_dir)], quiet=quiet)
        return

    if step_id == "e55_02_board_eval_engine":
        script_path = project_root / "tools" / "e55_02_board_eval_engine" / "run_e55_02_board_eval_engine_v1.py"
        _run_py_script(script_path, ["--workspace", str(position_dir)], quiet=quiet)
        return

    raise ValueError(f"unknown step_id: {step_id}")


def _run_pos_work_order(position_dir: Path, quiet: bool = False) -> None:
    wo_path = position_dir / "work_order.json"
    if not wo_path.exists():
        found = False
        for i in range(20):
            time.sleep(0.05 * (i + 1))
            if wo_path.exists():
                found = True
                break
        if not found:
            raise FileNotFoundError(f"work_order.json not found: {wo_path}")

    wo = _read_json(wo_path)
    steps = wo.get("steps", [])
    if not isinstance(steps, list):
        raise ValueError("work_order.steps invalid")

    if not steps:
        return

    project_root = _infer_project_root_from_pos(position_dir)
    workspaces_root = _infer_workspaces_root_from_pos(position_dir)

    for s in steps:
        step_id = str(s.get("id"))
        ver = str(s.get("version", "v6"))
        t0 = time.perf_counter()
        _log_step("START", position_dir, step_id, ver, quiet=quiet)
        _run_step(step_id, ver, position_dir, project_root=project_root, workspaces_root=workspaces_root, quiet=quiet)
        dt_ms = int((time.perf_counter() - t0) * 1000.0)
        _log_step("DONE", position_dir, step_id, ver, dt_ms, quiet=quiet)




def _read_e08_overall_delete_judge(position_dir: Path) -> bool:
    p = position_dir / "step_data" / "e08_20_delete_rule_judge.v1.json"
    d = _read_json(p)
    return bool(d.get("overall_delete_judge", False))


def _read_e11_02_decision(position_dir: Path) -> bool:
    p = position_dir / "step_data" / "e11_02_reading_continuation_decision_engine.v1.json"
    d = _read_json(p)
    return bool(d.get("decision", False))


def _build_next_ranks(instruction_rank: Optional[int], position_dir: Path) -> List[int]:
    if not isinstance(instruction_rank, int):
        return []
    if 1 <= instruction_rank <= 5:
        return [instruction_rank + 1]
    if instruction_rank == 6:
        overall = _read_e08_overall_delete_judge(position_dir)
        return [18 if overall else 9]
    if instruction_rank == 9:
        decision = _read_e11_02_decision(position_dir)
        return [10 if decision else 18]
    return []


def _build_rank10_allocation_plan(position_dir: Path) -> Dict[str, Any]:
    e01_06_path = position_dir / "step_data" / "e01_06_execute_moves_promotion_split.v1.json"
    e01_06 = _read_json(e01_06_path)

    candidates = e01_06.get("candidates", {})
    if not isinstance(candidates, dict):
        candidates = {}

    out_candidates: Dict[str, Any] = {}
    has_any_candidates = False

    for promo_key in ("as_is", "promote"):
        blocks = candidates.get(promo_key, [])
        if not isinstance(blocks, list):
            blocks = []

        out_blocks: List[Dict[str, Any]] = []
        for blk in blocks:
            if not isinstance(blk, dict):
                continue
            to_list = blk.get("to", [])
            if not isinstance(to_list, list):
                to_list = []
            new_blk = dict(blk)
            new_blk.pop("to_pos", None)
            new_blk["to_pos_count"] = len(to_list)
            out_blocks.append(new_blk)

        if out_blocks:
            has_any_candidates = True
        out_candidates[promo_key] = out_blocks

    return {
        "schema": "rank10_allocation_plan.v1",
        "from_pos_id": position_dir.name,
        "candidates": out_candidates,
        "has_any_candidates": has_any_candidates,
    }
def _write_instruction_done(position_dir: Path, instruction_rank: int) -> Path:
    workspaces_root = _infer_workspaces_root_from_pos(position_dir)
    p = workspaces_root / "current" / "instruction_done.json"
    obj = {
        "schema": "instruction_done.v6",
        "finished_at": _now_iso(),
        "instruction_rank": int(instruction_rank),
        "position_dir": str(position_dir),
    }
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def _worker_loop(work_q: "Queue[Optional[WorkItem]]", result_q: "Queue[WorkResult]", quiet: bool = False) -> None:
    while True:
        item = work_q.get()
        if item is None:
            return

        rank = item.artifacts.get("instruction_rank")
        position_dir = Path(str(item.artifacts.get("position_dir", "")))
        workspaces_root: Optional[Path] = None
        task_t0 = time.perf_counter()

        try:
            workspaces_root = _infer_workspaces_root_from_pos(position_dir)
            before_pos = set(_list_current_pos_names(workspaces_root))

            _run_pos_work_order(position_dir, quiet=quiet)

            after_pos = set(_list_current_pos_names(workspaces_root))
            new_pos = sorted([p for p in (after_pos - before_pos) if p != "pos000001"])

            if isinstance(rank, int):
                _write_instruction_done(position_dir, rank)
                if rank == 18:
                    _report_eval_to_search_tree(position_dir)

            from_pos_id = position_dir.name
            transitions: List[Dict[str, str]] = []
            for to_pos_id in new_pos:
                transitions.append(
                    {
                        "from_pos_id": str(from_pos_id),
                        "move": "",
                        "to_pos_id": str(to_pos_id),
                    }
                )

            allocation_plan: Dict[str, Any] = {}
            next_ranks = _build_next_ranks(rank if isinstance(rank, int) else None, position_dir)
            if isinstance(rank, int) and rank == 10:
                allocation_plan = _build_rank10_allocation_plan(position_dir)
                next_ranks = [11] if bool(allocation_plan.get("has_any_candidates", False)) else [18]

            elapsed_ms = int((time.perf_counter() - task_t0) * 1000.0)
            if isinstance(rank, int):
                _record_instruction_measure(workspaces_root, item.task_id, rank, position_dir, elapsed_ms, True)

            msg = json.dumps(
                {
                    "message": "ok",
                    "transitions": transitions,
                    "instruction_elapsed_ms": elapsed_ms,
                    "instruction_rank": rank if isinstance(rank, int) else None,
                    "position_dir": str(position_dir),
                },
                ensure_ascii=False,
            )

            result_q.put(
                WorkResult(
                    ok=True,
                    task_id=item.task_id,
                    message=msg,
                    instruction_rank=rank if isinstance(rank, int) else None,
                    position_dir=str(position_dir),
                    next_ranks=next_ranks,
                    new_positions=new_pos,
                    search_tree_updates=(
                        [{"kind": "update_eval", "pos_name": position_dir.name}]
                        if isinstance(rank, int) and rank == 18
                        else []
                    ),
                    allocation_plan=allocation_plan,
                )
            )

        except Exception as e:
            elapsed_ms = int((time.perf_counter() - task_t0) * 1000.0)
            if workspaces_root is None:
                try:
                    workspaces_root = _infer_workspaces_root_from_pos(position_dir)
                except Exception:
                    workspaces_root = None
            if isinstance(rank, int) and workspaces_root is not None:
                try:
                    _record_instruction_measure(workspaces_root, item.task_id, rank, position_dir, elapsed_ms, False)
                except Exception:
                    pass
            result_q.put(
                WorkResult(
                    ok=False,
                    task_id=item.task_id,
                    message=str(e),
                    instruction_rank=rank if isinstance(rank, int) else None,
                    position_dir=str(position_dir),
                )
            )


def start_worker_pool(
    worker_threads: int, work_q: "Queue[Optional[WorkItem]]", result_q: "Queue[WorkResult]", quiet: bool = False
) -> None:
    threads: List[threading.Thread] = []
    for _ in range(int(worker_threads)):
        t = threading.Thread(target=_worker_loop, args=(work_q, result_q, quiet), daemon=True)
        t.start()
        threads.append(t)