CREATE TABLE Z_Z_workstation (
  id      INTEGER      NOT NULL,
  guid    VARCHAR(150) NOT NULL
)
;

insert into Z_Z_workstation
select ws_id as id, guid from Z_Z_db_info
;

drop table Z_Z_db_info
;