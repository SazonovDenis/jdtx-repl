ALTER TABLE Z_Z_state ADD que_common_no INT DEFAULT 0 NOT NULL;


ALTER TABLE Z_Z_state_ws ADD que_out001_send_done INT DEFAULT 0 NOT NULL;


ALTER TABLE Z_Z_state_ws ADD que_out000_no INT DEFAULT 0 NOT NULL;


ALTER TABLE Z_Z_state_ws ADD que_out000_send_done INT DEFAULT 0 NOT NULL;


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

