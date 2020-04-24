alter table Z_Z_workstation add ws_id INTEGER NOT NULL;

update Z_Z_workstation set ws_id = id;

update Z_Z_workstation set id = 1;

