CREATE TABLE ${args.SYS_TABLE_PREFIX}verdb (
  id       INTEGER      NOT NULL,
  name     VARCHAR(100),           /* Имя модуля (опционально) */
  ver      INT          NOT NULL,  /* Номер текущей версии */
  ver_step INT          NOT NULL   /* Равна нулю, если смена версии выполнена полностью или равна номеру следующего шага, если не полностью */
);

CREATE UNIQUE INDEX ${args.SYS_TABLE_PREFIX}verdb_idx ON ${args.SYS_TABLE_PREFIX}verdb (name)
;

INSERT INTO ${args.SYS_TABLE_PREFIX}verdb (id, name, ver, ver_step) VALUES (1, '', 0, 0)
;
