CREATE TABLE Z_Z_age_tmp (
  age        INT         NOT NULL,
  dt         TIMESTAMP   NOT NULL,
  table_ids  blob
);


@Update_011_012_fill_age
;


drop TABLE Z_Z_age;


CREATE TABLE Z_Z_age (
  age        INT         NOT NULL,
  dt         TIMESTAMP   NOT NULL,
  table_ids  blob
);


insert into Z_Z_age
select
  Z_Z_age_tmp.age,
  Z_Z_age_tmp.dt,
  Z_Z_age_tmp.table_ids
from
  Z_Z_age_tmp
;


drop TABLE Z_Z_age_tmp;
