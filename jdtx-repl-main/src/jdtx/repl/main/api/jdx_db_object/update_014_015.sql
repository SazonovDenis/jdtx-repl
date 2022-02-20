/*
WS_STATE.que_out_no_done -> WS_STATE.audit_age_done
*/

alter table Z_Z_WS_STATE add audit_age_done INT NOT NULL;

update Z_Z_WS_STATE set audit_age_done = que_out_no_done;

alter table Z_Z_WS_STATE drop que_out_no_done;



/*
Z_Z_que_in
Z_Z_que_in001
*/

ALTER TABLE Z_Z_que_in add author_ws_id INT NOT NULL;

ALTER TABLE Z_Z_que_in add author_id INT NOT NULL;

update Z_Z_que_in set author_ws_id = ws_id;

alter table Z_Z_que_in drop ws_id;



ALTER TABLE Z_Z_que_in001 add author_ws_id INT NOT NULL;

ALTER TABLE Z_Z_que_in001 add author_id INT NOT NULL;

update Z_Z_que_in001 set author_ws_id = ws_id;

alter table Z_Z_que_in001 drop ws_id;



/*
Z_Z_srv_que_in
*/

CREATE TABLE Z_Z_srv_que_in (
  id                 INTEGER NOT NULL,
  author_ws_id       INT     NOT NULL,
  author_id          INT     NOT NULL,
  age                INT     NOT NULL,
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_in ADD CONSTRAINT pk_Z_Z_srv_que_in PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_in_idx ON Z_Z_srv_que_in (author_ws_id, author_id);

CREATE generator Z_Z_G_srv_que_in;

SET generator Z_Z_G_srv_que_in TO 0;



/*
Z_Z_QUE_COMMON   ->   Z_Z_SRV_QUE_COMMON
Z_Z_QUE_OUT000   ->   Z_Z_SRV_QUE_OUT000
Z_Z_QUE_OUT001   ->   Z_Z_SRV_QUE_OUT001

Очереди реплик. Общая очередь (формируется на сервере)
*/

CREATE TABLE Z_Z_srv_que_common (
  id                 INTEGER NOT NULL,
  author_ws_id       INT     NOT NULL,
  author_id          INT     NOT NULL,
  age                INT     NOT NULL,
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_common ADD CONSTRAINT pk_Z_Z_srv_que_common PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_common_idx ON Z_Z_srv_que_common (author_ws_id, author_id);

insert into Z_Z_SRV_QUE_COMMON
select
  id,
  ws_id as author_ws_id,
  -1 as author_id,
  age,
  crc,
  replica_type
from
  Z_Z_QUE_COMMON
;


CREATE TABLE Z_Z_srv_que_out000 (
  id                 INTEGER NOT NULL,
  destination_ws_id  INT     NOT NULL,
  destination_id     INT     NOT NULL,
  author_ws_id       INT     NOT NULL,
  author_id          INT     NOT NULL,
  age                INT     NOT NULL,
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_out000 ADD CONSTRAINT pk_Z_Z_srv_que_out000 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_out000_idx ON Z_Z_srv_que_out000 (destination_ws_id, destination_id);

CREATE generator Z_Z_G_srv_que_out000;

insert into Z_Z_SRV_QUE_OUT000
select
  id,
  destination_ws_id,
  destination_no as destination_id,
  ws_id as author_ws_id,
  -1 as author_id,
  age,
  crc,
  replica_type
from
  Z_Z_QUE_OUT000
;


CREATE TABLE Z_Z_srv_que_out001 (
  id                 INTEGER NOT NULL,  
  destination_ws_id  INT     NOT NULL,
  destination_id     INT     NOT NULL,
  author_ws_id       INT     NOT NULL,
  author_id          INT     NOT NULL,
  age                INT     NOT NULL,
  crc                VARCHAR(32),
  replica_type       INT     NOT NULL
);

ALTER TABLE Z_Z_srv_que_out001 ADD CONSTRAINT pk_Z_Z_srv_que_out001 PRIMARY KEY (id);

CREATE UNIQUE INDEX Z_Z_srv_que_out001_idx ON Z_Z_srv_que_out001 (destination_ws_id, destination_id);

CREATE generator Z_Z_G_srv_que_out001;

insert into Z_Z_SRV_QUE_OUT001
select
  id,
  destination_ws_id,
  destination_no as destination_id,
  ws_id as author_ws_id,
  -1 as author_id,
  age,
  crc,
  replica_type
from
  Z_Z_QUE_OUT001
;


drop table Z_Z_QUE_OUT000;

drop generator Z_Z_G_que_out000;

drop table Z_Z_QUE_OUT001;

drop generator Z_Z_G_que_out001;

drop table Z_Z_QUE_COMMON;

drop generator Z_Z_G_que_common;


@Update_014_015_que_generators;
