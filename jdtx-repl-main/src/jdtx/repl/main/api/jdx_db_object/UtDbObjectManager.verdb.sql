CREATE TABLE Z_Z_verdb (
  id       INTEGER      NOT NULL,
  name     VARCHAR(100) NOT NULL,  /* Имя модуля (опционально) */
  ver      INT          NOT NULL,  /* Номер текущей версии */
  ver_step INT          NOT NULL   /* Равна нулю, если смена версии выполнена полностью или равна номеру следующего шага, если не полностью */
);

INSERT INTO Z_Z_verdb (id, name, ver, ver_step) VALUES (1, '', 0, 0)
;
