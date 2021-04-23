ALTER TABLE Z_Z_state add que_in_no                 INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state add que_in001_no              INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state add que_in001_no_done         INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state add que_common_no             INT DEFAULT 0 NOT NULL;



ALTER TABLE Z_Z_state_ws add que_out000_no          INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws add que_out000_send_done   INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws add que_out001_no          INT DEFAULT 0 NOT NULL;

ALTER TABLE Z_Z_state_ws add que_out001_send_done   INT DEFAULT 0 NOT NULL;



CREATE TABLE Z_Z_que_in001 (
  id           INTEGER NOT NULL,
  ws_id        INT     NOT NULL,
  age          INT     NOT NULL,
  replica_type INT     NOT NULL
);

ALTER TABLE Z_Z_que_in001 ADD CONSTRAINT pk_Z_Z_que_in001 PRIMARY KEY (id);

CREATE generator Z_Z_G_que_in001;

SET generator Z_Z_G_que_in001 TO 0;



CREATE TABLE Z_Z_que_out001 (
  id                  INTEGER NOT NULL,
  destination_ws_id   INT     NOT NULL,
  destination_no      INT     NOT NULL,
  ws_id               INT     NOT NULL,
  age                 INT     NOT NULL,
  replica_type        INT     NOT NULL
);

ALTER TABLE Z_Z_que_out001 ADD CONSTRAINT pk_Z_Z_que_out001 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_que_out001_idx ON Z_Z_que_out001 (destination_ws_id, destination_no);

CREATE generator Z_Z_G_que_out001;

SET generator Z_Z_G_que_out001 TO 0;



CREATE TABLE Z_Z_que_out000 (
  id                  INTEGER NOT NULL,
  destination_ws_id   INT     NOT NULL,
  destination_no      INT     NOT NULL,
  ws_id               INT     NOT NULL,
  age                 INT     NOT NULL,
  replica_type        INT     NOT NULL
);

ALTER TABLE Z_Z_que_out000 ADD CONSTRAINT pk_Z_Z_que_out000 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_que_out000_idx ON Z_Z_que_out000 (destination_ws_id, destination_no);

CREATE generator Z_Z_G_que_out000;

SET generator Z_Z_G_que_out000 TO 0;