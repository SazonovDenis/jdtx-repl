package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtPublicationRule {


    static Log log = LogFactory.getLog("jdtx.PublicationRule");

    public static List<String> expandPublicationFields(IJdxTable table, String publicationFields) {
        List<String> res = new ArrayList<>();

        //
        // JdxDbUtils.ID_FIELD пусть будет всегда спереди (необязательно, но... во-первых это красиво!)
        res.add(JdxDbUtils.ID_FIELD);
        if (publicationFields.contains("*")) {
            publicationFields = publicationFields.toUpperCase() + ","; // Добавляем "," чтобы корректно работало publicationFields.contains(....
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
     * Проверка: если в правиле упомянута таблица, то все таблицы, НА КОТОРЫЕ она ссылается - тоже должны быть упомянуты
     */
    public static void checkValidRef(IPublicationStorage publication, IJdxDbStruct struct) {
        Collection<IPublicationRule> rules = publication.getPublicationRules();
        for (IPublicationRule rule : rules) {
            IJdxTable table = struct.getTable(rule.getTableName());
            for (IJdxForeignKey fieldFk : table.getForeignKeys()) {
                IJdxTable tableRef = fieldFk.getTable();
                String refFieldName = fieldFk.getField().getName();
                //
                boolean refTableNotFoundInRules = false;
                if (tableRef != null) {
                    IPublicationRule ruleTable = publication.getPublicationRule(tableRef.getName());
                    if (ruleTable == null) {
                        refTableNotFoundInRules = true;
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
                        log.error("Not found reference in rule: " + table.getName() + "." + refFieldName + " -> " + tableRef.getName());
                    } else {
                        log.info("Masked reference in rule: " + table.getName() + "." + refFieldName + " -> " + tableRef.getName());
                    }
                }
            }
        }
    }


}
