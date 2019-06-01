/*
jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.sql
*/

/* таблица с флагом работы триггеров */
CREATE TABLE Z_Z_flag_tab (
  id           INTEGER NOT NULL,
  trigger_flag INTEGER NOT NULL
);

INSERT INTO Z_Z_flag_tab (id, trigger_flag) VALUES (1, 1);


/* таблица собственного состояния (для рабочей станции) - для хранения возраста созданных реплик, примененных реплик и т.п. */
CREATE TABLE Z_Z_state (
  id                     INTEGER NOT NULL,
  que_out_age_done       INT     NOT NULL,
  que_in_no_done         INT     NOT NULL,
  mail_send_done         INT     NOT NULL,
  enabled                INT     NOT NULL,
  mute                   INT     NOT NULL,
  db_struct_fixed        BLOB,
  db_struct_allowed      BLOB
);

INSERT INTO Z_Z_state (id, que_out_age_done, que_in_no_done, mail_send_done, enabled, mute) VALUES (1, 0, 0, 0, 0, 0);


/* таблица состояния рабочих станций (для сервера) - хранение возраста созданных реплик, примененных реплик и т.п. */
CREATE TABLE Z_Z_state_ws (
  id                       INTEGER NOT NULL,
  ws_id                    INT     NOT NULL,
  que_common_dispatch_done INT     NOT NULL,
  que_in_age_done          INT     NOT NULL,
  enabled                  INT     NOT NULL,
  mute_age                 INT     NOT NULL
);

ALTER TABLE Z_Z_state_ws ADD CONSTRAINT pk_Z_Z_state_ws PRIMARY KEY (id);

CREATE generator Z_Z_G_state_ws;


/* список рабочих станций (для сервера) */
CREATE TABLE Z_Z_workstation_list (
  id      INTEGER      NOT NULL,
  name    VARCHAR(50)  NOT NULL,
  guid    VARCHAR(150) NOT NULL
);

ALTER TABLE Z_Z_workstation_list ADD CONSTRAINT pk_Z_Z_workstation_list PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_workstation_list_idx1 ON Z_Z_workstation_list (name);

CREATE UNIQUE INDEX Z_Z_workstation_list_idx2 ON Z_Z_workstation_list (guid);


/* очереди реплик - входящая */
CREATE TABLE Z_Z_que_in (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_in ADD CONSTRAINT pk_Z_Z_que_in PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in;

SET generator Z_Z_G_que_in TO 0;


/* очереди реплик - исходящая */
CREATE TABLE Z_Z_que_out (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_out ADD CONSTRAINT pk_Z_Z_que_out PRIMARY KEY (id);

CREATE generator Z_Z_G_que_out;

SET generator Z_Z_G_que_out TO 0;


/* очереди реплик - общая (для сервера) */
CREATE TABLE Z_Z_que_common (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_common ADD CONSTRAINT pk_Z_Z_que_common PRIMARY KEY (id);

CREATE generator Z_Z_G_que_common;

SET generator Z_Z_G_que_common TO 0;


/* таблица для хранения возраста таблиц */
CREATE TABLE Z_Z_age (
  age        INT         NOT NULL,
  table_name VARCHAR(50) NOT NULL,
  Z_id       INT         NOT NULL,
  dt         TIMESTAMP   NOT NULL
);


/* метка с номером БД */
CREATE TABLE Z_Z_db_info (
  ws_id   INTEGER      NOT NULL,
  guid    VARCHAR(150) NOT NULL
);

INSERT INTO Z_Z_db_info (ws_id, guid) VALUES (0, '');



/*
jdtx/repl/main/api/jdx_db_object/UtDbObjectDecodeManager.sql
*/

CREATE TABLE Z_Z_decode (
  ws_id      INTEGER,
  table_name VARCHAR(150),
  ws_slot    INTEGER,
  own_slot   INTEGER
);

CREATE UNIQUE INDEX Z_Z_decode_idx1 ON Z_Z_decode (ws_id, table_name, ws_slot);

CREATE UNIQUE INDEX Z_Z_decode_idx2 ON Z_Z_decode (table_name, own_slot);