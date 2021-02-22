/*
Таблица с флагом работы триггеров
*/
CREATE TABLE Z_Z_flag_tab (
  id           INTEGER NOT NULL,
  trigger_flag INTEGER NOT NULL
);

INSERT INTO Z_Z_flag_tab (id, trigger_flag) VALUES (1, 1);


/*
Таблица для хранения возраста таблиц
*/
CREATE TABLE Z_Z_age (
  age        INT         NOT NULL,
  table_name VARCHAR(50) NOT NULL,
  Z_id       INT         NOT NULL,
  dt         TIMESTAMP   NOT NULL
);

CREATE UNIQUE INDEX Z_Z_age_idx ON Z_Z_age (age, table_name);


/*
Собственное состояния (для рабочей станции или сервера) -
для хранения возраста: созданных реплик, примененных реплик и т.п.
*/
CREATE TABLE Z_Z_state (
  id                     INTEGER NOT NULL,
  que_out_no             INT     DEFAULT 0 NOT NULL,
  que_in_no              INT     DEFAULT 0 NOT NULL,
  que_in001_no           INT     DEFAULT 0 NOT NULL,
  que_out_age_done       INT     NOT NULL,
  que_in_no_done         INT     NOT NULL,
  que_in001_no_done      INT     DEFAULT 0 NOT NULL,
  que_common_no          INT     DEFAULT 0 NOT NULL,
  mail_send_done         INT     NOT NULL,
  enabled                INT     NOT NULL,
  mute                   INT     NOT NULL
);

INSERT INTO Z_Z_state (id, que_out_age_done, que_in001_no_done, que_in_no_done, mail_send_done, enabled, mute) VALUES (1, 0, 0, 0, 0, 0, 0);


/*
Состояния рабочих станций (для сервера) -
хранение возраста созданных реплик, примененных реплик и т.п.
*/
CREATE TABLE Z_Z_state_ws (
  id                       INTEGER NOT NULL,
  ws_id                    INT     NOT NULL,
  que_common_dispatch_done INT     NOT NULL,
  que_out000_no            INT     DEFAULT 0 NOT NULL,
  que_out000_send_done     INT     DEFAULT 0 NOT NULL,
  que_out001_no            INT     DEFAULT 0 NOT NULL,
  que_out001_send_done     INT     DEFAULT 0 NOT NULL,
  que_in_age_done          INT     NOT NULL,            /* Возраст реплики, до которого обработана входящая очередь от рабочей станции */
  enabled                  INT     NOT NULL,
  mute_age                 INT     NOT NULL
);

ALTER TABLE Z_Z_state_ws ADD CONSTRAINT pk_Z_Z_state_ws PRIMARY KEY (id);

CREATE generator Z_Z_G_state_ws;


/*
Метка с номером БД и настройками (для рабочей станции)
*/
CREATE TABLE Z_Z_workstation (
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

INSERT INTO Z_Z_workstation (id, ws_id, guid) VALUES (1, 0, '');


/*
Список рабочих станций и их собственных настроек (для сервера)
*/
CREATE TABLE Z_Z_workstation_list (
  id               INTEGER      NOT NULL,
  name             VARCHAR(50)  NOT NULL,
  guid             VARCHAR(150) NOT NULL,
  cfg_publications BLOB,
  cfg_decode       BLOB,
  cfg_ws           BLOB
);

ALTER TABLE Z_Z_workstation_list ADD CONSTRAINT pk_Z_Z_workstation_list PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_workstation_list_idx1 ON Z_Z_workstation_list (name);

CREATE UNIQUE INDEX Z_Z_workstation_list_idx2 ON Z_Z_workstation_list (guid);


/*
Очереди реплик - входящая на рабочей станции in
*/
CREATE TABLE Z_Z_que_in (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_in ADD CONSTRAINT pk_Z_Z_que_in PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in;

SET generator Z_Z_G_que_in TO 0;


/*
Очереди реплик - входящая на рабочей станции in001
*/
CREATE TABLE Z_Z_que_in001 (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_in001 ADD CONSTRAINT pk_Z_Z_que_in001 PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in001;

SET generator Z_Z_G_que_in001 TO 0;


/*
Очереди реплик - исходящая от сервера out001
*/
CREATE TABLE Z_Z_que_out001 (
  id                  INTEGER NOT NULL,
  destination_ws_id   INT     NOT NULL,  /* Станция - получатель */
  destination_no      INT     NOT NULL,  /* Номер для получателя, монотонно растет */
  ws_id               INT     NOT NULL,
  age                 INT     NOT NULL,
  replica_type        INT     NOT NULL
);

ALTER TABLE Z_Z_que_out001 ADD CONSTRAINT pk_Z_Z_que_out001 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_que_out001_idx ON Z_Z_que_out001 (destination_ws_id, destination_no);

CREATE generator Z_Z_G_que_out001;

SET generator Z_Z_G_que_out001 TO 0;


/*
Очереди реплик - исходящая от сервера out000 (продукт обработки общей очереди common -> out000 для каждой станции)
*/
CREATE TABLE Z_Z_que_out000 (
  id                  INTEGER NOT NULL,
  destination_ws_id   INT     NOT NULL,  /* Станция - получатель */
  destination_no      INT     NOT NULL,  /* Номер для получателя, монотонно растет */
  ws_id               INT     NOT NULL,
  age                 INT     NOT NULL,
  replica_type        INT     NOT NULL
);

ALTER TABLE Z_Z_que_out000 ADD CONSTRAINT pk_Z_Z_que_out000 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_que_out000_idx ON Z_Z_que_out000 (destination_ws_id, destination_no);

CREATE generator Z_Z_G_que_out000;

SET generator Z_Z_G_que_out000 TO 0;


/*
Очереди реплик - исходящая (рабочая станция)
*/
CREATE TABLE Z_Z_que_out (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_out ADD CONSTRAINT pk_Z_Z_que_out PRIMARY KEY (id);

CREATE generator Z_Z_G_que_out;

SET generator Z_Z_G_que_out TO 0;


/*
Очереди реплик - общая (на сервере)
*/
CREATE TABLE Z_Z_que_common (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_common ADD CONSTRAINT pk_Z_Z_que_common PRIMARY KEY (id);

CREATE generator Z_Z_G_que_common;

SET generator Z_Z_G_que_common TO 0;



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