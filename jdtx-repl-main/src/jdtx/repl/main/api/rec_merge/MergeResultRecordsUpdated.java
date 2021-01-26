package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

/**
 * Обновленные записи в таблице
 */
public class MergeResultRecordsUpdated {

    /**
     * Таблица, чьи записи обновили, чтобы сделать merge в основной таблице
     */
    //String tableName;

    /**
     * По какому полю искали в таблице tableName
     */
    String refFieldName;

    /**
     * Обновленные записи в таблице tableName
     */
    DataStore recordsUpdated;

}
