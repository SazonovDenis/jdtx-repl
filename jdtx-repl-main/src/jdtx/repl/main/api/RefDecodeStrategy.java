package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.web.*;
import org.json.simple.*;

import java.util.*;

/**
 */
public class RefDecodeStrategy {

    int NO_DECODE = 1;
    int DECODE_ID = 2;

    public static RefDecodeStrategy instance = null;

    protected Map<String, RefDecodeStrategyItem> tablesDecodeStrategy;

    public void init(String cfgFileName) throws Exception {
        JSONArray data = (JSONArray) UtJson.toObject(UtFile.loadString(cfgFileName));

        // Стратегии перекодировки каждой таблицы
        tablesDecodeStrategy = new HashMap<>();

        // Загрузим стратегии перекодировки
        for (Object o : data) {
            JSONObject st = (JSONObject) o;
            String tableName = (String) st.get("table");
            tableName = tableName.toUpperCase();
            RefDecodeStrategyItem tableDecodeStrategyItem = new RefDecodeStrategyItem();
            switch ((String) st.get("strategy")) {
                case "DECODE_ID":
                    tableDecodeStrategyItem.strategy = DECODE_ID;
                    tableDecodeStrategyItem.decode_from_id = (Long) st.get("decode_from_id");
                    break;
                case "NO_DECODE":
                default:
                    tableDecodeStrategyItem.strategy = NO_DECODE;
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
