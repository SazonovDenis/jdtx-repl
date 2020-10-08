package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

/**
 * Обновленные записи в зависимой таблице
 */
public class MergeResultRefTable {

    /**
     * Таблица, чьи записи обновили, чтобы сделать merge в основной таблице
     */
    String refTtableName;

    /**
     * По какому полю искали в таблице refTtableName
     */
    String refTtableRefFieldName;

    /**
     * Обновленные записи в  таблице refTtableName
     */
    DataStore recordsUpdated;

}
