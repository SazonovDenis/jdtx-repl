package jdtx.repl.main.api.rec_merge;

import java.util.*;

/**
 * Задача на слияние дубликатов в таблице tableName.
 * Имеет список удаляемых записей и одну эталонную запись, которую оставляем.
 */
public class RecMergeTask {

    /**
     * Таблица, для которой делаем merge
     */
    String tableName;

    /**
     * Запись (эталонная), корторая остается в таблице tableName взамен удаленных, теперь все будут ссылатся на нее
     */
    Map recordEtalon;

    /**
     * Удаляемые записи в таблице tableName
     */
    Collection<Long> recordsDelete;

    /**
     *
     */
    public RecMergeTask() {
        recordsDelete = new ArrayList<>();
    }

}
