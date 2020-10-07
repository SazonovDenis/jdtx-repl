package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import java.util.*;

class RecDuplicate {
    /**
     * Поля и их значения - ключ для поикса дублей
     * Name -> МВД РК
     */
    Map params;

    /**
     * Список записей, содержащих дубликаты
     */
    DataStore records;
}

class RecMergeTask {
    /**
     * Таблица, чьи записи делаем merge
     */
    String tableName;

    /**
     * Запись (эталонная), корторая остается взамен удаленных, теперь все будут ссылатся на нее
     */
    DataRecord recordEtalon;

    /**
     * Удаляемые записи в нашей таблице
     */
    Collection<Long> recordsDelete;

    public RecMergeTask() {
        recordsDelete = new ArrayList<>();
    }
}

class RecMergeResultRefTable {
    /**
     * Таблица, чьи записи обновили, чтобы сделать merge в основной таблице
     */
    String refTtableName;
    String refTtableRefFieldName;

    /**
     * Обновленные записи в каждой зависимой таблице (которые ссылаются на основную таблицу)
     */
    DataStore recordsUpdated;
}