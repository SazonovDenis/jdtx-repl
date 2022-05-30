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
  dt         TIMESTAMP   NOT NULL,
  table_ids  blob
);

CREATE UNIQUE INDEX Z_Z_age_idx ON Z_Z_age (age);



/*
Собственное состояние для рабочей станции
*/
CREATE TABLE Z_Z_WS_STATE (
  id                INTEGER       NOT NULL,
  age               INT DEFAULT 0 NOT NULL,    /* Возраст собственного аудита рабочей станции */
  que_in_no         INT DEFAULT 0 NOT NULL,    /* Возраст очереди in */
  que_in001_no      INT DEFAULT 0 NOT NULL,    /* Возраст очереди in001 */
  que_out_no        INT DEFAULT 0 NOT NULL,    /* Возраст очереди out */
  que_in_no_done    INT DEFAULT 0 NOT NULL,    /* Метка использования/обработки очереди in */
  que_in001_no_done INT DEFAULT 0 NOT NULL,    /* Метка использования/обработки очереди in001 */
  audit_age_done    INT DEFAULT 0 NOT NULL,    /* Метка формирования очереди out (ВОЗРАСТ аудита, до которого сформирована исходящая очередь out) */
  mail_send_done    INT DEFAULT 0 NOT NULL,    /* Метка отправки почты (из очереди out) */
  mute              INT DEFAULT 0 NOT NULL     /* Состояние MUTE */
);

INSERT INTO Z_Z_WS_STATE (id) VALUES (1);



/*
Собственное состояние для сервера.
Для сведения:
Поле que_common_no_done (возраст, до которого обработана общая очередь) не нужно в этой таблице,
т.к. очередь queCommon раскидывается на queOut000, а состояние этого процесса свое для каждой станции
и хранится в Z_Z_SRV_WORKSTATION_STATE
*/
CREATE TABLE Z_Z_SRV_STATE (
  id                INTEGER       NOT NULL,
  /* Возраст очередей */
  que_common_no     INT DEFAULT 0 NOT NULL
);

INSERT INTO Z_Z_SRV_STATE (id) VALUES (1);



/*
Состояния рабочих станций (для сервера) -
хранение возраста созданных реплик, примененных реплик и т.п.
*/
CREATE TABLE Z_Z_SRV_WORKSTATION_STATE (
  id                       INTEGER NOT NULL,
  ws_id                    INT     NOT NULL,
  que_common_dispatch_done INT     NOT NULL,            /* Номер реплики, до которого реплики из queCommon разложены в queOut000 для рабочей станции */
  que_out000_no            INT     DEFAULT 0 NOT NULL,
  que_out000_send_done     INT     DEFAULT 0 NOT NULL,
  que_out001_no            INT     DEFAULT 0 NOT NULL,
  que_out001_send_done     INT     DEFAULT 0 NOT NULL,
  que_in_no                INT     NOT NULL,            /* Номер реплики, до которого принята очередь от рабочей станции (в серверное зеркало queInSrv) */
  que_in_no_done           INT     NOT NULL,            /* Номер реплики, до которого обработана очередь от рабочей станции (при формировании общей очереди) */
  enabled                  INT     NOT NULL,
  mute_age                 INT     NOT NULL             /* Номер реплики в queCommon, которой станция отправила подтверждение своего состояния MUTE. Пока станция не выйдет из состояния MUTE, новых реплик в queCommon от этой станции не появится */
);

ALTER TABLE Z_Z_SRV_WORKSTATION_STATE ADD CONSTRAINT PK_Z_Z_SRV_WORKSTATION_STATE PRIMARY KEY (id);


/*
Метка с номером БД и настройками (для рабочей станции)
*/
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

INSERT INTO Z_Z_WS_INFO (id, ws_id, guid) VALUES (1, 0, '');



/*
Список рабочих станций и их собственных настроек (для сервера)
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



/*
Очереди реплик. QueIn - входящая на рабочей станции
*/
CREATE TABLE Z_Z_que_in (
  id                 INTEGER NOT NULL,  /* Номер реплики, монотонно растет, равен номеру Z_Z_srv_que_common.id */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_que_in ADD CONSTRAINT pk_Z_Z_que_in PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in;

SET generator Z_Z_G_que_in TO 0;



/*
Очереди реплик. QueIn001 - входящая на рабочей станции
*/
CREATE TABLE Z_Z_que_in001 (
  id                 INTEGER NOT NULL,  /* Номер реплики, монотонно растет, равен номеру Z_Z_srv_que_out001.destination_id */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_que_in001 ADD CONSTRAINT pk_Z_Z_que_in001 PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in001;

SET generator Z_Z_G_que_in001 TO 0;


/*
Очереди реплик. QueOut - исходящая от рабочей станции
*/
CREATE TABLE Z_Z_que_out (
  id                 INTEGER NOT NULL, /* Номер реплики, монотонно растет, присваивает эта рабочая станция */
  ws_id              INT     NOT NULL, /* Станция-автор (эта рабочая станция) */
  age                INT     NOT NULL, /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_que_out ADD CONSTRAINT pk_Z_Z_que_out PRIMARY KEY (id);

CREATE generator Z_Z_G_que_out;

SET generator Z_Z_G_que_out TO 0;


/*
Очереди реплик. Зеркальная входящая очередь (формируется на сервере, реплики от рабочих станций)
*/
CREATE TABLE Z_Z_srv_que_in (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_in ADD CONSTRAINT pk_Z_Z_srv_que_in PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_in_idx ON Z_Z_srv_que_in (author_ws_id, author_id);

CREATE generator Z_Z_G_srv_que_in;

SET generator Z_Z_G_srv_que_in TO 0;



/*
Очереди реплик. Общая очередь (формируется на сервере)
*/
CREATE TABLE Z_Z_srv_que_common (
  id                 INTEGER NOT NULL,  /* Номер реплики в ОБЩЕЙ очереди, монотонно растет, присваивает сервер */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_common ADD CONSTRAINT pk_Z_Z_srv_que_common PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_common_idx ON Z_Z_srv_que_common (author_ws_id, author_id);

--CREATE generator Z_Z_G_srv_que_common;

--SET generator Z_Z_G_srv_que_common TO 0;



/*
Очереди реплик. QueOut000 - исходящая от сервера. Продукт обработки общей очереди common -> out000 для каждой станции
*/
CREATE TABLE Z_Z_srv_que_out000 (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  destination_ws_id  INT     NOT NULL,  /* Станция-получатель */
  destination_id     INT     NOT NULL,  /* Номер для получателя, монотонно растет */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_out000 ADD CONSTRAINT pk_Z_Z_srv_que_out000 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_out000_idx1 ON Z_Z_srv_que_out000 (destination_ws_id, destination_id);

CREATE UNIQUE INDEX Z_Z_srv_que_out000_idx2 ON Z_Z_srv_que_out000 (destination_ws_id, author_ws_id, author_id);

CREATE generator Z_Z_G_srv_que_out000;

SET generator Z_Z_G_srv_que_out000 TO 0;



/*
Очереди реплик. QueOut001 - исходящая от сервера
*/
CREATE TABLE Z_Z_srv_que_out001 (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  destination_ws_id  INT     NOT NULL,  /* Станция-получатель */
  destination_id     INT     NOT NULL,  /* Номер реплики для получателя, монотонно растет, присваивает сервер */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем: монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_out001 ADD CONSTRAINT pk_Z_Z_srv_que_out001 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_out001_idx1 ON Z_Z_srv_que_out001 (destination_ws_id, destination_id);

-- Индекс author_ws_id+author_id для очереди out001 не нужен, т.к. в нее мы можем положить реплиики,
-- которые НЕ пришли из других очередей, следовательно у них может быть НЕ УКАЗАНО значение author_id
-- Также может быть не указано значение author_ws_id - для серверных команд.
--CREATE UNIQUE INDEX Z_Z_srv_que_out001_idx2 ON Z_Z_srv_que_out001 (destination_ws_id, author_ws_id, author_id)

CREATE generator Z_Z_G_srv_que_out001;

SET generator Z_Z_G_srv_que_out001 TO 0;


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


