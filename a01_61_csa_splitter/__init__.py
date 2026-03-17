# -*- coding: utf-8 -*-
"""
a01_61_csa_splitter (Ver.6)

dev入口：
workspaces/root_state.json（CSA_RECORD）を入力し、
workspaces/current/pos000001/ に
  - root_state_61.json（CSA_RECORD保持：改変しない）
  - root_state_csa.json（turn_sign/nonturn_sign：+/-のみ）
  - work_order.json（次工程：a01_62→m07_00）
を生成する。
"""
