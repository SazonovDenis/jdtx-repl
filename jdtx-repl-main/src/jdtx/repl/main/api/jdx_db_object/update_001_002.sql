CREATE TABLE Z_Z_workstation (
  id      INTEGER      NOT NULL,
  guid    VARCHAR(150) NOT NULL,
  app_version_allowed    VARCHAR(150),
  db_struct_fixed        BLOB,
  db_struct_allowed      BLOB
);

insert into Z_Z_workstation
select
  Z_Z_db_info.ws_id as id,
  Z_Z_db_info.guid,
  Z_Z_state.app_version_allowed,
  Z_Z_state.db_struct_fixed,
  Z_Z_state.db_struct_allowed
from
  Z_Z_db_info, Z_Z_state
;

drop table Z_Z_db_info
;

alter table Z_Z_state drop app_version_allowed
;

alter table Z_Z_state drop db_struct_fixed
;

alter table Z_Z_state drop db_struct_allowed
;
