CREATE TABLE Z_Z_SRV_WORKSTATION_STATE_TMP (
  id          INTEGER     NOT NULL,
  ws_id       INT         NOT NULL,
  param_name  VARCHAR(50) NOT NULL,
  param_value INT         NOT NULL
);


@Update_015_016_srv_workstation_state;


DROP TABLE Z_Z_SRV_WORKSTATION_STATE;

CREATE TABLE Z_Z_SRV_WORKSTATION_STATE (
  id          INTEGER     NOT NULL,
  ws_id       INT         NOT NULL,
  param_name  VARCHAR(50) NOT NULL,
  param_value INT         NOT NULL
);

ALTER TABLE Z_Z_SRV_WORKSTATION_STATE ADD CONSTRAINT PK_Z_Z_SRV_WORKSTATION_STATE PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_SRV_WORKSTATION_STATE_IDX ON Z_Z_SRV_WORKSTATION_STATE (ws_id, param_name);


insert into Z_Z_SRV_WORKSTATION_STATE
select
  id,
  ws_id,
  param_name,
  param_value
from
  Z_Z_SRV_WORKSTATION_STATE_TMP
;


DROP TABLE Z_Z_SRV_WORKSTATION_STATE_TMP;
