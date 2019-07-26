package jdtx.repl.main.api.decoder;

import jandcode.utils.error.*;
import org.json.simple.*;

import java.util.*;

/**
 */
public class RefDecodeStrategy {

    int NO_DECODE = 1;
    int DECODE_ID = 2;

    public static RefDecodeStrategy instance = null;

    protected Map<String, RefDecodeStrategyItem> tablesDecodeStrategy;

    public void init(JSONObject cfgDecode) throws Exception {
        // Стратегии перекодировки каждой таблицы
        tablesDecodeStrategy = new HashMap<>();

        // Загрузим стратегии перекодировки
        for (Object k : cfgDecode.keySet()) {
            String tableName = (String) k;
            tableName = tableName.toUpperCase();
            JSONObject st = (JSONObject) cfgDecode.get(k);

            //
            RefDecodeStrategyItem tableDecodeStrategyItem = new RefDecodeStrategyItem();
            String strategy = (String) st.get("strategy");
            switch (strategy) {
                case "DECODE_ID":
                    tableDecodeStrategyItem.strategy = DECODE_ID;
                    tableDecodeStrategyItem.decode_from_id = (Long) st.get("decode_from_id");
                    break;
                case "NO_DECODE":
                    tableDecodeStrategyItem.strategy = NO_DECODE;
                    break;
                default:
                    throw new XError("Unknown strategy: " + strategy + ", tableName: " + tableName);
            }
            tablesDecodeStrategy.put(tableName, tableDecodeStrategyItem);
        }
    }

    // todo: возможно есть более сложное поведение. Это может быть перекрыто в кастомных обработчиках
    protected boolean needDecode(String tableName, long ws_id, long db_id) {
        RefDecodeStrategyItem strategyItem = tablesDecodeStrategy.get(tableName);

        // Для PS: id находится в нерегулируемом диапазоне.
        if (db_id <= 0) {
            return false;
        }

        // Стратегия не задана - по умолчанию перекодировать
        if (strategyItem == null) {
            return true;
        }

        // Стратегия "не перекодировать"
        if (strategyItem.strategy == NO_DECODE) {
            return false;
        }

        // Стратегия "перекодировать" - проверяем диапазон
        if (strategyItem.strategy == DECODE_ID && db_id < strategyItem.decode_from_id) {
            return false;
        }

        //
        return true;
    }

}
