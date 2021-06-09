/*
Z_Z_WORKSTATION_LIST   ->  Z_Z_SRV_WORKSTATION_LIST
Z_Z_STATE_WS           ->  Z_Z_SRV_WORKSTATION_STATE
Z_Z_STATE              ->  Z_Z_SRV_STATE
                       ->  Z_Z_WS_STATE
Z_Z_WORKSTATION        ->  Z_Z_WS_INFO
*/


CREATE TABLE Z_Z_SRV_WORKSTATION_LIST (
  id               INTEGER      NOT NULL,
  name             VARCHAR(50)  NOT NULL,
  guid             VARCHAR(150) NOT NULL,
  cfg_publications BLOB,
  cfg_decode       BLOB,
  cfg_ws           BLOB
);

ALTER TABLE Z_Z_SRV_WORKSTATION_LIST ADD CONSTRAINT PK_Z_Z_SRV_WORKSTATION_LIST PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_SRV_WORKSTATION_LIST_IDX1 ON Z_Z_SRV_WORKSTATION_LIST (name);

CREATE UNIQUE INDEX Z_Z_SRV_WORKSTATION_LIST_IDX2 ON Z_Z_SRV_WORKSTATION_LIST (guid);


insert into Z_Z_SRV_WORKSTATION_LIST
select
  id,
  name,
  guid,
  cfg_publications,
  cfg_decode,
  cfg_ws
from
  Z_Z_WORKSTATION_LIST
;




CREATE TABLE Z_Z_SRV_WORKSTATION_STATE (
  id                       INTEGER NOT NULL,
  ws_id                    INT     NOT NULL,
  que_common_dispatch_done INT     NOT NULL,
  que_out000_no            INT     DEFAULT 0 NOT NULL,
  que_out000_send_done     INT     DEFAULT 0 NOT NULL,
  que_out001_no            INT     DEFAULT 0 NOT NULL,
  que_out001_send_done     INT     DEFAULT 0 NOT NULL,
  que_in_no_done           INT     NOT NULL,
  enabled                  INT     NOT NULL,
  mute_age                 INT     NOT NULL
);

ALTER TABLE Z_Z_SRV_WORKSTATION_STATE ADD CONSTRAINT PK_Z_Z_SRV_WORKSTATION_STATE PRIMARY KEY (id);

CREATE generator Z_Z_G_SRV_WORKSTATION_STATE;


insert into Z_Z_SRV_WORKSTATION_STATE
select
  id,
  ws_id,
  que_common_dispatch_done,
  que_out000_no,
  que_out000_send_done,
  que_out001_no,
  que_out001_send_done,
  que_in_age_done as que_in_no_done,
  enabled,
  mute_age
from
    Z_Z_STATE_WS
;



CREATE TABLE Z_Z_SRV_STATE (
  id                INTEGER       NOT NULL,
  que_common_no     INT DEFAULT 0 NOT NULL
);


insert into Z_Z_SRV_STATE
select
  id,
  que_common_no
from
  Z_Z_STATE
;



CREATE TABLE Z_Z_WS_STATE (
  id                INTEGER       NOT NULL,
  que_in_no         INT DEFAULT 0 NOT NULL,
  que_in001_no      INT DEFAULT 0 NOT NULL,
  que_in_no_done    INT DEFAULT 0 NOT NULL,
  que_in001_no_done INT DEFAULT 0 NOT NULL,
  que_out_no_done      INT DEFAULT 0 NOT NULL,
  mail_send_done    INT DEFAULT 0 NOT NULL,
  mute              INT DEFAULT 0 NOT NULL
);


insert into Z_Z_WS_STATE
select
  id,
  que_in_no,
  que_in001_no,
  que_in_no_done,
  que_in001_no_done,
  que_out_age_done as que_out_no_done,
  mail_send_done,
  mute
from
  Z_Z_STATE
;



CREATE TABLE Z_Z_WS_INFO (
  id                  INTEGER      NOT NULL,
  ws_id               INTEGER      NOT NULL,
  guid                VARCHAR(150) NOT NULL,
  app_version_allowed VARCHAR(150),
  db_struct_fixed     BLOB,
  db_struct_allowed   BLOB,
  cfg_publications    BLOB,
  cfg_decode          BLOB,
  cfg_ws              BLOB
);


insert into Z_Z_WS_INFO
select
  id,
  ws_id,
  guid,
  app_version_allowed,
  db_struct_fixed,
  db_struct_allowed,
  cfg_publications,
  cfg_decode,
  cfg_ws
from
  Z_Z_WORKSTATION
;




drop table Z_Z_WORKSTATION_LIST
;

drop table Z_Z_STATE_WS
;

drop table Z_Z_STATE
;

drop table Z_Z_WORKSTATION
;

