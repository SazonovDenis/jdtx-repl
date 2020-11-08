ALTER TABLE Z_Z_state ADD que_out_no INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state ADD que_in_no INT DEFAULT -1 NOT NULL;

ALTER TABLE Z_Z_state ADD que_in001_no INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws ADD que_common_no INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws ADD que_out001_no INT DEFAULT 0 NOT NULL;


update Z_Z_state set que_out_no = (
  select (case when (select max(ID) from Z_Z_que_out) is null then 0 else (select max(id) from Z_Z_que_out) end) from DUAL
);

update Z_Z_state set que_in_no = (
  select (case when (select max(ID) from Z_Z_que_in) is null then -1 else (select max(id) from Z_Z_que_in) end) from DUAL
);

update Z_Z_state set que_in001_no = (
  select (case when (select max(id) from Z_Z_que_in001) is null then 0 else (select max(id) from Z_Z_que_in001) end) from DUAL
);

update Z_Z_state_ws set que_common_no = (
  select (case when (select max(ID) from Z_Z_que_common) is null then 0 else (select max(id) from Z_Z_que_common) end) from DUAL
);

update Z_Z_state_ws set que_out001_no = (
  select (case when (select max(id) from Z_Z_que_out001) is null then 0 else (select max(id) from Z_Z_que_out001) end) from DUAL
);
