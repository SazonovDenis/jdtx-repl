ALTER TABLE Z_Z_state ADD que_in001_no_done INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws ADD snapshot_age INT DEFAULT 0 NOT NULL;


/*
Очереди реплик - входящая на рабочей станции In001
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
Очереди реплик - исходящая от сервера Out001
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
