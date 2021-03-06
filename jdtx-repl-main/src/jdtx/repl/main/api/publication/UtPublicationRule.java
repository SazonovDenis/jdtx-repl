package jdtx.repl.main.api.publication;

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
        // JdxDbUtils.ID_FIELD пусть будет всегда спереди (необязательно, но... во-первых это красиво!)
        res.add(JdxDbUtils.ID_FIELD);
        if (publicationFields.contains("*")) {
            // Вариант "Все, кроме...", например "*,-CommentUsr"
            publicationFields = publicationFields.toUpperCase() + ","; // Добавляем "," чтобы корректно работало publicationFields.contains("-" + fieldName + ",")
            for (IJdxField fieldStruct : table.getFields()) {
                String fieldName = fieldStruct.getName().toUpperCase();
                //
                if (fieldName.equalsIgnoreCase(JdxDbUtils.ID_FIELD)) {
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
                if (publicationField.equalsIgnoreCase(JdxDbUtils.ID_FIELD)) {
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
    public static void checkValid(JSONObject cfgPublicationRules, IPublicationStorage publication, IJdxDbStruct struct) {
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
                IJdxTable tableRef = fieldFk.getTable();
                String refTableName = tableRef.getName();
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

        // Проверка: Таблицы из структуры, которые игнорируем
        for (IJdxTable table : struct.getTables()) {
            IPublicationRule rule = publication.getPublicationRule(table.getName());
            if (rule == null) {
                log.info("Rule not found for table: " + table.getName());
            }
        }
    }


}
