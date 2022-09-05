package jdtx.repl.main.api.ref_manager;

import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class UtDecodeStrategy {

    static Log log = LogFactory.getLog("jdtx.DecodeStrategy");

    public static void checkValid(RefDecodeStrategy decodeStrategy, IJdxDbStruct struct) {
        // Проверка: Правило для таблицы в decodeStrategy, которой нет в структуре
        for (String tableName : decodeStrategy.tablesDecodeStrategy.keySet()) {
            if (struct.getTable(tableName) == null) {
                log.warn("Not found table in struct, table: " + tableName);
            }
        }
    }

}
