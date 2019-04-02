alter table Z_Z_state
  add enabled INTEGER NOT NULL
;


update Z_Z_state set enabled = (select enabled from Z_Z_db_info)
;


alter table Z_Z_db_info
  drop enabled
;