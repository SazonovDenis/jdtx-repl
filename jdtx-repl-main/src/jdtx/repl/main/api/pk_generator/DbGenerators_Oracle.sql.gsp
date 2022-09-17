CREATE OR REPLACE PROCEDURE ${args.SYS_TABLE_PREFIX}sequence_set_value(seq_name IN VARCHAR2, new_value IN NUMBER)
AS
  inc_by NUMBER;
  curr_value NUMBER;
  diff_val NUMBER;
BEGIN
  -- Узнаем и запомним настройку INCREMENT_BY для SEQUENCE
  EXECUTE IMMEDIATE 'SELECT increment_by FROM user_sequences WHERE sequence_name = ''' || seq_name || '''' INTO inc_by;

  -- Узнаем какое значение сейчас считается будущим (путем вызова XXX.NEXTVAL)
  EXECUTE IMMEDIATE 'SELECT ' || seq_name || '.NEXTVAL FROM dual' INTO curr_value;

  -- Узнаем какое значение нужно прибавить к нынешнему, чтобы будущим стало считаться new_value
  diff_val := new_value - curr_value;

  -- Не нужно двигать?
  IF diff_val = 0 THEN
    RETURN;
  END IF;

  -- Перенастроим настройку INCREMENT_BY для SEQUENCE и установим значение путем вызова XXX.NEXTVAL
  EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || seq_name || ' INCREMENT BY ' || diff_val;

  EXECUTE IMMEDIATE 'SELECT ' || seq_name || '.NEXTVAL FROM dual' INTO curr_value;

  -- Восстановим течение SEQUENCE - вернем настройку INCREMENT_BY
  EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || seq_name || ' INCREMENT BY ' || inc_by;

END ${args.SYS_TABLE_PREFIX}sequence_set_value;
