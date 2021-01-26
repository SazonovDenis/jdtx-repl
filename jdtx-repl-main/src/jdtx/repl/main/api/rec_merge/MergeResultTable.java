package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

/**
 * Результат слияния записей-дубликатов в таблицы.
 * Записи, затронутые слиянием - как удаленные из самой таблице(recordsDeleted),
 * так и обновленные в зависимых таблицах (recordsUpdated)
 */
public class MergeResultTable {

    /**
     * Удаленные записи в самой таблице (как были до удаления)
     */
    DataStore recordsDeleted;

    /**
     * Обновленные записи в зависимых таблицах:
     * что пришлось сделать с каждой из зависимых таблиц, чтобы сделать merge в основной таблице.
     */
    RecordsUpdatedMap recordsUpdated = new RecordsUpdatedMap();

}
