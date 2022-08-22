package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.util.*;

public class UtPublicationRule {


    static Log log = LogFactory.getLog("jdtx.PublicationRule");

    public static List<String> expandPublicationFields(IJdxTable table, String publicationFields) {
        List<String> res = new ArrayList<>();

        //
        // PK пусть будет всегда спереди (необязательно, но... во-первых это красиво!)
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
        res.add(pkFieldName);
        if (publicationFields.contains("*")) {
            // Вариант "Все, кроме...", например "*,-CommentUsr"
            publicationFields = publicationFields.toUpperCase() + ","; // Добавляем "," чтобы корректно работало publicationFields.contains("-" + fieldName + ",")
            for (IJdxField fieldStruct : table.getFields()) {
                String fieldName = fieldStruct.getName().toUpperCase();
                //
                if (fieldName.equalsIgnoreCase(pkFieldName)) {
                    continue;
                }
                //
                if (!publicationFields.contains("-" + fieldName + ",")) {
                    res.add(fieldName);
                }
            }

            // Проверим, что в фильтре не указаны несуществующие поля
            String[] publicationFieldsArr = publicationFields.split(",");
            for (String publicationField : publicationFieldsArr) {
                if (publicationField.startsWith("-")) {
                    publicationField = publicationField.substring(1);
                    if (table.getField(publicationField) == null) {
                        log.warn("Not found ignored field in struct, field: " + table.getName() + "." + publicationField);
                    }
                }
            }
        } else {
            // Вариант "Перечислены нужные поля через запятую"
            String[] publicationFieldsArr = publicationFields.split(",");
            for (String publicationField : publicationFieldsArr) {
                if (publicationField.equalsIgnoreCase(pkFieldName)) {
                    continue;
                }
                // Проверим, что в фильтре не указаны несуществующие поля
                if (table.getField(publicationField) == null) {
                    log.warn("Not found field in struct, field: " + table.getName() + "." + publicationField);
                    continue;
                }
                //
                res.add(publicationField);
            }
        }

        //
        return res;
    }

    /**
     * Проверки
     */
    public static void checkValid(JSONObject cfgPublicationRules, IPublicationRuleStorage publication, IJdxDbStruct struct) {
        // Проверка: Правило для таблицы в cfgPublicationRules, которой нет в структуре
        for (Object key : cfgPublicationRules.keySet()) {
            String tableName = (String) key;
            if (struct.getTable(tableName) == null) {
                log.warn("Not found table in struct, table: " + tableName);
            }
        }

        // Проверка: если в правиле упомянута таблица, то все таблицы, НА КОТОРЫЕ она ссылается - тоже должны быть упомянуты
        Collection<IPublicationRule> rules = publication.getPublicationRules();
        for (IPublicationRule rule : rules) {
            IJdxTable table = struct.getTable(rule.getTableName());
            for (IJdxForeignKey fieldFk : table.getForeignKeys()) {
                IJdxTable refTable = fieldFk.getTable();
                String refTableName = refTable.getName();
                String refFieldName = fieldFk.getField().getName();
                //
                boolean refTableNotFoundInRules = false;
                boolean refTableFilteredInRules = false;
                IPublicationRule refTableRule = publication.getPublicationRule(refTableName);
                if (refTableRule == null) {
                    refTableNotFoundInRules = true;
                } else {
                    String ruleFilter = rule.getFilterExpression();
                    String refTableRuleFilter = refTableRule.getFilterExpression();
                    if (refTableRuleFilter != null && !refTableRuleFilter.equals(ruleFilter)) {
                        refTableFilteredInRules = true;
                    }
                }


                //
                boolean refFieldFoundInRuleFields = false;
                for (IJdxField ruleField : rule.getFields()) {
                    if (ruleField.getName().compareToIgnoreCase(refFieldName) == 0) {
                        refFieldFoundInRuleFields = true;
                    }
                }

                if (refTableNotFoundInRules) {
                    if (refFieldFoundInRuleFields) {
                        log.error("Not found reference in rule: " + table.getName() + "." + refFieldName + " -> " + refTableName);
                    } else {
                        log.info("Masked reference in rule: " + table.getName() + "." + refFieldName + " -> " + refTableName);
                    }
                } else if (refTableFilteredInRules) {
                    log.warn("Reference filtered in rule: " + table.getName() + "." + refFieldName + " -> " + refTableName);
                }
            }
        }

        // Проверка: Таблицы из структуры, которые не упомянуты в правилах
        for (IJdxTable table : struct.getTables()) {
            IPublicationRule rule = publication.getPublicationRule(table.getName());
            if (rule == null) {
                log.info("Rule not found for table: " + table.getName());
            }
        }
    }

    /**
     * Находит разницу между двумя наборами правил
     *
     * @param publicationRules1 первая структура для сравнения
     * @param publicationRules2 вторая структура для сравнения
     * @param addded            возвращает таблицы во второй структуре (publicationRules2), которых нет в первой (publicationRules1)
     * @param removed           возвращает таблицы в первой структуре, которых нет во второй
     * @param changed           возвращает таблицы, которые есть в обеих, но с измененными правилами
     */
    public static void getPublicationRulesDiff(IJdxDbStruct struct, IPublicationRuleStorage publicationRules1, IPublicationRuleStorage publicationRules2, List<IJdxTable> addded, List<IJdxTable> removed, List<IJdxTable> changed) {
        for (IPublicationRule publicationRule2 : publicationRules2.getPublicationRules()) {
            IPublicationRule publicationRule1 = publicationRules1.getPublicationRule(publicationRule2.getTableName());
            if (publicationRule1 == null) {
                // Таблица появилась в новых правилах
                addded.add(struct.getTable(publicationRule2.getTableName()));
            }
        }

        for (IPublicationRule publicationRule2 : publicationRules2.getPublicationRules()) {
            IPublicationRule publicationRule1 = publicationRules1.getPublicationRule(publicationRule2.getTableName());
            if (publicationRule1 != null && !UtJdxData.equals(publicationRule1.getFilterExpression(), publicationRule2.getFilterExpression())) {
                // таблица c измененным expression в новых правилах
                changed.add(struct.getTable(publicationRule2.getTableName()));
            }
        }

        for (IPublicationRule publicationRule1 : publicationRules1.getPublicationRules()) {
            IPublicationRule publicationRule2 = publicationRules2.getPublicationRule(publicationRule1.getTableName());
            if (publicationRule2 == null) {
                // Таблица удалена в новых правилах
                removed.add(struct.getTable(publicationRule1.getTableName()));
            }
        }
    }

}
