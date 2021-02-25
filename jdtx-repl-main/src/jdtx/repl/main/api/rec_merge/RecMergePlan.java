package jdtx.repl.main.api.rec_merge;

import java.util.*;

/**
 * План (задача) на слияние дубликатов в таблице tableName.
 * Имеет список удаляемых записей и одну эталонную запись, которую оставляем.
 */
public class RecMergePlan {

    /**
     * Таблица, для которой делаем merge
     */
    String tableName;

    /**
     * Запись (эталонная), корторая появится в таблице tableName взамен удаленных, теперь все будут ссылатся на нее
     */
    Map<String, Object> recordEtalon;

    /**
     * Удаляемые записи в таблице tableName
     */
    Collection<Long> recordsDelete;

    /**
     *
     */
    public RecMergePlan() {
        recordsDelete = new ArrayList<>();
    }

}
