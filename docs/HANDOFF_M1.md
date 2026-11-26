# M1 handoff

Core implementation preview submitted. The initial CI compile reached the MySQL module and reported ambiguous ChangeEvent imports, fixed in this follow-up. Source/target identity checks, preflight-only state handling and bounded shutdown wait improved. CI now includes an isolated MySQL 8.4 integration script (snapshot, DML, key change, restart and temporary target-table failure). Results must be checked before marking M1 complete. Crash-consistent snapshot recovery, log expiry, retention/GC and expanded types remain pending.
