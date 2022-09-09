package jdtx.repl.main.api.ref_manager;

import jandcode.utils.error.*;
import org.json.simple.*;

import java.util.*;

/**
 * Хранилище настроек стратегий перекодировки,
 * используется в jdtx.repl.main.api.decoder.RefManagerDecode
 */
public class RefDecodeStrategy {

    int NO_DECODE = 1;
    int DECODE_ID = 2;

    /**
     * Хранилище устроено так:
     * TableA: {strategy: DECODE_ID, decode_from_id: 1000},
     * TableB: {strategy: NO_DECODE}
     */
    protected Map<String, RefDecodeStrategyItem> tablesDecodeStrategy;


    public void init(JSONObject cfg) throws Exception {
        // Стратегии перекодировки каждой таблицы
        tablesDecodeStrategy = new HashMap<>();

        // Загрузим стратегии перекодировки
        if (cfg != null) {
            for (Object key : cfg.keySet()) {
                String tableName = (String) key;
                tableName = tableName.toUpperCase();
                JSONObject st = (JSONObject) cfg.get(key);

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
    }


    /**
     * @param tableName Имя таблицы
     * @param db_id     Значение id
     * @return Нужно ли перекодировать локальный (свой собственный) id
     * <p>
     * todo: Это поведение для PS. Возможно есть более сложное поведение, тогда это должно быть перекрыто в кастомных обработчиках
     */
    protected boolean needDecodeOwn(String tableName, long db_id) {
        tableName = tableName.toUpperCase();

        //
        RefDecodeStrategyItem tableDecodeStrategy = tablesDecodeStrategy.get(tableName);

        // todo: ТОЛЬКО Для PS: id находится в нерегулируемом диапазоне.
        if (db_id <= 0) {
            return false;
        }

        // Стратегия для таблицы не задана - по умолчанию перекодировать
        if (tableDecodeStrategy == null) {
            return true;
        }

        // Стратегия "не перекодировать"
        if (tableDecodeStrategy.strategy == NO_DECODE) {
            return false;
        }

        // Стратегия "перекодировать" - проверяем диапазон, начиная с которого надо перекодировать
        if (tableDecodeStrategy.strategy == DECODE_ID && db_id < tableDecodeStrategy.decode_from_id) {
            return false;
        }

        //
        return true;
    }

}