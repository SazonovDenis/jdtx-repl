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
    Map<String, String> recordEtalon;

    /**
     * Удаляемые записи (Id) в таблице tableName
     */
    Collection<Long> recordsDelete;

    /**
     *
     */
    public RecMergePlan() {
        recordEtalon = new HashMap<>();
        recordsDelete = new ArrayList<>();
    }

}
