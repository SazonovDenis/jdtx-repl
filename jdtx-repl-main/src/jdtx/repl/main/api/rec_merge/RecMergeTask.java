package jdtx.repl.main.api.rec_merge;

import java.util.*;

public class RecMergeTask {
    /**
     * Таблица, чьи записи делаем merge
     */
    String tableName;

    /**
     * Запись (эталонная), корторая остается взамен удаленных, теперь все будут ссылатся на нее
     */
    Map recordEtalon;

    /**
     * Удаляемые записи в нашей таблице
     */
    Collection<Long> recordsDelete;

    public RecMergeTask() {
        recordsDelete = new ArrayList<>();
    }
}
