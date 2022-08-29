/*
Таблица с флагом работы триггеров
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}flag_tab (
  id           INTEGER NOT NULL,
  trigger_flag INTEGER NOT NULL
);

INSERT INTO ${args.SYS_TABLE_PREFIX}flag_tab (id, trigger_flag) VALUES (1, 1);


/*
Таблица для хранения возраста таблиц
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}age (
  age        INT         NOT NULL,
  dt         TIMESTAMP   NOT NULL,
  table_ids  blob
);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}age_idx ON ${args.SYS_TABLE_PREFIX}age (age);



/*
Собственное состояние для рабочей станции
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}WS_STATE (
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

INSERT INTO ${args.SYS_TABLE_PREFIX}WS_STATE (id) VALUES (1);



/*
Собственное состояние для сервера.
Для сведения - поле que_common_no_done (возраст, до которого обработана общая очередь) не нужно в этой таблице,
т.к. очередь queCommon раскидывается на queOut000, а состояние этого процесса свое для каждой станции
и хранится в ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}SRV_STATE (
  id                INTEGER       NOT NULL,
  /* Возраст очередей */
  que_common_no     INT DEFAULT 0 NOT NULL
);

INSERT INTO ${args.SYS_TABLE_PREFIX}SRV_STATE (id) VALUES (1);



/*
Состояния рабочих станций (для сервера) -
хранение возраста созданных реплик, примененных реплик и т.п.
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE (
  id          INTEGER     NOT NULL,
  ws_id       INT         NOT NULL,
  param_name  VARCHAR(50) NOT NULL,
  param_value INT         NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE_IDX ON ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_STATE (ws_id, param_name);



/*
Метка с номером БД и настройками (для рабочей станции)
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}WS_INFO (
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

INSERT INTO ${args.SYS_TABLE_PREFIX}WS_INFO (id, ws_id, guid) VALUES (1, 0, '-');



/*
Список рабочих станций и их собственных настроек (для сервера)
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST (
  id               INTEGER      NOT NULL,
  name             VARCHAR(50)  NOT NULL,
  guid             VARCHAR(150) NOT NULL,
  db_struct        BLOB,
  cfg_publications BLOB,
  cfg_decode       BLOB,
  cfg_ws           BLOB
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST_IDX1 ON ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST (name);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST_IDX2 ON ${args.SYS_TABLE_PREFIX}SRV_WORKSTATION_LIST (guid);



/*
Очереди реплик. QueIn - входящая на рабочей станции
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}que_in (
  id                 INTEGER NOT NULL,  /* Номер реплики, монотонно растет, равен номеру ${args.SYS_TABLE_PREFIX}srv_que_common.id */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}que_in ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}que_in PRIMARY KEY (id);


CREATE SEQUENCE ${args.SYS_GEN_PREFIX}que_in MINVALUE 0 START WITH 0 INCREMENT BY 1;




/*
Очереди реплик. QueIn001 - входящая на рабочей станции
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}que_in001 (
  id                 INTEGER NOT NULL,  /* Номер реплики, монотонно растет, равен номеру ${args.SYS_TABLE_PREFIX}srv_que_out001.destination_id */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}que_in001 ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}que_in001 PRIMARY KEY (id);

CREATE SEQUENCE ${args.SYS_GEN_PREFIX}que_in001 MINVALUE 0 START WITH 0 INCREMENT BY 1;



/*
Очереди реплик. QueOut - исходящая от рабочей станции
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}que_out (
  id                 INTEGER NOT NULL, /* Номер реплики, монотонно растет, присваивает эта рабочая станция */
  ws_id              INT     NOT NULL, /* Станция-автор (эта рабочая станция) */
  age                INT     NOT NULL, /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}que_out ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}que_out PRIMARY KEY (id);

CREATE SEQUENCE ${args.SYS_GEN_PREFIX}que_out MINVALUE 0 START WITH 0 INCREMENT BY 1;



/*
Очереди реплик. Зеркальная входящая очередь (формируется на сервере, реплики от рабочих станций)
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}srv_que_in (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}srv_que_in ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}srv_que_in PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_in_idx ON ${args.SYS_TABLE_PREFIX}srv_que_in (author_ws_id, author_id);

CREATE SEQUENCE ${args.SYS_GEN_PREFIX}srv_que_in MINVALUE 0 START WITH 0 INCREMENT BY 1;



/*
Очереди реплик. Общая очередь (формируется на сервере)
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}srv_que_common (
  id                 INTEGER NOT NULL,  /* Номер реплики в ОБЩЕЙ очереди, монотонно растет, присваивает сервер */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}srv_que_common ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}srv_que_common PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_common_idx ON ${args.SYS_TABLE_PREFIX}srv_que_common (author_ws_id, author_id);




/*
Очереди реплик. QueOut000 - исходящая от сервера. Продукт обработки общей очереди common -> out000 для каждой станции
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}srv_que_out000 (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  destination_ws_id  INT     NOT NULL,  /* Станция-получатель */
  destination_id     INT     NOT NULL,  /* Номер для получателя, монотонно растет */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}srv_que_out000 ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}srv_que_out000 PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_out000_idx1 ON ${args.SYS_TABLE_PREFIX}srv_que_out000 (destination_ws_id, destination_id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_out000_idx2 ON ${args.SYS_TABLE_PREFIX}srv_que_out000 (destination_ws_id, author_ws_id, author_id);

CREATE SEQUENCE ${args.SYS_GEN_PREFIX}srv_que_out000 MINVALUE 0 START WITH 0 INCREMENT BY 1;



/*
Очереди реплик. QueOut001 - исходящая от сервера
*/
CREATE TABLE ${args.SYS_TABLE_PREFIX}srv_que_out001 (
  id                 INTEGER NOT NULL,  /* Просто id, не имеет значения*/
  destination_ws_id  INT     NOT NULL,  /* Станция-получатель */
  destination_id     INT     NOT NULL,  /* Номер реплики для получателя, монотонно растет, присваивает сервер */
  author_ws_id       INT     NOT NULL,  /* Станция-автор */
  author_id          INT     NOT NULL,  /* Номер реплики у автора, проверяем, что монотонно растет для автора */
  age                INT     NOT NULL,  /* Данные о реплике */
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE ${args.SYS_TABLE_PREFIX}srv_que_out001 ADD CONSTRAINT PK_${args.SYS_TABLE_PREFIX}srv_que_out001 PRIMARY KEY (id);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_out001_idx1 ON ${args.SYS_TABLE_PREFIX}srv_que_out001 (destination_ws_id, destination_id);

-- Индекс author_ws_id+author_id для очереди out001 не нужен, т.к. в нее мы можем положить реплиики,
-- которые НЕ пришли из других очередей, следовательно у них может быть НЕ УКАЗАНО значение author_id
-- Также может быть не указано значение author_ws_id - для серверных команд.
--CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}srv_que_out001_idx2 ON ${args.SYS_TABLE_PREFIX}srv_que_out001 (destination_ws_id, author_ws_id, author_id)

CREATE SEQUENCE ${args.SYS_GEN_PREFIX}srv_que_out001 MINVALUE 0 START WITH 0 INCREMENT BY 1;
