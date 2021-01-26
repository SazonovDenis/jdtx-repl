package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

/**
 * Обновленные записи в таблице
 */
public class RecordsUpdated {

    /**
     * Таблица, чьи записи обновили, чтобы сделать merge в основной таблице
     */
    String refTableName;

    /**
     * По какому полю искали в таблице refTableName
     */
    public String refFieldName;

    /**
     * Обновленные записи в таблице tableName
     */
    public DataStore recordsUpdated;

}
