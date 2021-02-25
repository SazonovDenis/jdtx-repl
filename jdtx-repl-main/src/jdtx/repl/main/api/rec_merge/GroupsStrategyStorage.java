package jdtx.repl.main.api.rec_merge;

import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.util.*;

/**
 * Хранилище правил GroupStrategy.
 */
public class GroupsStrategyStorage {

    /**
     * Хранилище устроено так:
     * TableA: [ f1:[f1,f2,f3], f2:[f1,f2,f3], f3:[f1,f2,f3] ],
     * TableB: [ f5:[f5,f6], f6:[f5,f6] ]
     */
    Map<String, GroupStrategy> strategyStorage = new HashMap<>();

    //
    protected static Log log = LogFactory.getLog("jdtx.FieldsGroupsStrategy");


    /**
     * @return Возвращает правила группировки (группы связанныех полей) для таблицы tableName
     */
    public GroupStrategy getForTable(String tableName) {
        if (strategyStorage.containsKey(tableName)) {
            // Есть правило - возвращаем его для таблицы tableName
            return strategyStorage.get(tableName);
        } else {
            // Нет правила - возвращаем пустое правило
            return new GroupStrategy();
        }
    }


    public void loadStrategy(JSONObject cfg, IJdxDbStruct structActual) {
        strategyStorage.clear();

        for (Object key : cfg.keySet()) {
            JSONArray tableStrategysJson = (JSONArray) cfg.get(key);
            String tableName = (String) key;

            //
            IJdxTable structTable = structActual.getTable(tableName);
            if (structTable == null) {
                // Правило для таблицы, которой нет в структуре
                log.warn("Not found table in struct: " + tableName);
                continue;
            }

            GroupStrategy groupStrategy = new GroupStrategy();
            strategyStorage.put(structTable.getName(), groupStrategy);

            for (Object o : tableStrategysJson) {
                String strategyTableStr = (String) o;
                String[] strategyTableArr = strategyTableStr.split(",");
                Collection<String> strategyTableList = new ArrayList<>();

                for (String fieldName : strategyTableArr) {
                    IJdxField field = structTable.getField(fieldName);
                    if (field == null) {
                        log.warn("Not found field in table struct: " + tableName + "." + fieldName);
                        continue;
                    }
                    strategyTableList.add(field.getName());
                }

                // Добавляем список полей в группе для каждого поля (ключа field)
                for (String fieldName : strategyTableList) {
                    groupStrategy.put(fieldName, strategyTableList);
                }
            }
        }
    }


}
