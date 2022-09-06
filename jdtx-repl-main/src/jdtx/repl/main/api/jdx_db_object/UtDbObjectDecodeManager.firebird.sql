CREATE TABLE Z_Z_decode (
  ws_id      INTEGER,
  table_name VARCHAR(150),
  ws_slot    INTEGER,
  own_slot   INTEGER
);

CREATE UNIQUE INDEX Z_Z_decode_idx1 ON Z_Z_decode (ws_id, table_name, ws_slot);

CREATE UNIQUE INDEX Z_Z_decode_idx2 ON Z_Z_decode (table_name, own_slot);