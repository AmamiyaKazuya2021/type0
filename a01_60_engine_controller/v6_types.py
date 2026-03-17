# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class WorkItem:
    task_id: str
    artifacts: Dict[str, Any]


@dataclass(frozen=True)
class WorkResult:
    task_id: str
    ok: bool
    message: str = ""
    failed_step_id: Optional[str] = None
    instruction_rank: Optional[int] = None
    position_dir: str = ""
    next_ranks: List[int] = field(default_factory=list)
    new_positions: List[str] = field(default_factory=list)
    hold_reason: str = ""
    search_tree_updates: List[Dict[str, Any]] = field(default_factory=list)
    allocation_plan: Dict[str, Any] = field(default_factory=dict)
    postprocess_request: str = ""
