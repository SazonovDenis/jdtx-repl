package jdtx.repl.main.api.rec_merge;

import java.util.*;

public class MergeResultTableMap extends HashMap<String, MergeResultTable> {

    /**
     * Возвращает MergeResultTable для таблицы tableName
     * или добавляет новый, если такого нет RecordsUpdated
     */
    public MergeResultTable addForTable(String tableName) {
        MergeResultTable item = get(tableName);
        //
        if (item == null) {
            item = new MergeResultTable();
            put(tableName, item);
        }
        //
        return item;
    }


}
