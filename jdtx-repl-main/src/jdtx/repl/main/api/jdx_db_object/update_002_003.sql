alter table Z_Z_state_ws
  add enabled INT NOT NULL
;


update Z_Z_state_ws set enabled = 1
;


alter table Z_Z_workstation_list
  drop enabled
;