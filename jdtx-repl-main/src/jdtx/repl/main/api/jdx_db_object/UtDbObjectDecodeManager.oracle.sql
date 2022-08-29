CREATE TABLE ${args.SYS_TABLE_PREFIX}decode (
  ws_id      INTEGER,
  table_name VARCHAR(150),
  ws_slot    INTEGER,
  own_slot   INTEGER
);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}decode_idx1 ON ${args.SYS_TABLE_PREFIX}decode (ws_id, table_name, ws_slot);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}decode_idx2 ON ${args.SYS_TABLE_PREFIX}decode (table_name, own_slot);


