package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.util.*;

public class PublicationStorage implements IPublicationStorage {

    //
    protected static Log log = LogFactory.getLog("jdtx.PublicationStorage");

    //
    private Collection<IPublicationRule> publicationRules = new ArrayList<>();

    @Override
    public void loadRules(JSONObject cfg, IJdxDbStruct baseStruct) throws Exception {
        publicationRules.clear();

        for (Object key : cfg.keySet()) {
            JSONObject publicationRuleJson = (JSONObject) cfg.get(key);
            String publicationTableName = (String) key;

            //
            IJdxTable structTable = baseStruct.getTable(publicationTableName);
            if (structTable == null) {
                // Правило для таблицы, которой нет в структуре
                log.warn("Not found table in struct, table: " + publicationTableName);
                continue;
            }

            // Добавляем правило
            IPublicationRule publicationRule = new PublicationRule();
            publicationRules.add(publicationRule);

            // IPublicationRule.setTableName
            publicationRule.setTableName(publicationTableName);

            // IPublicationRule.setFields
            List<String> publicationFields = expandPublicationFields(structTable, (String) publicationRuleJson.get("fields"));
            for (String publicationFieldName : publicationFields) {
                IJdxField publicationField = structTable.getField(publicationFieldName).cloneField();
                publicationRule.getFields().add(publicationField);
            }

            // IPublicationRule.setAuthorWs
            publicationRule.setAuthorWs((String) publicationRuleJson.getOrDefault("authorWs", null));
        }
    }

    @Override
    public Collection<IPublicationRule> getPublicationRules() {
        return publicationRules;
    }

    @Override
    public IPublicationRule getPublicationRule(String tableName) {
        for (IPublicationRule rule : publicationRules) {
            if (rule.getTableName().compareToIgnoreCase(tableName) == 0) {
                return rule;
            }
        }
        return null;
    }


    private List<String> expandPublicationFields(IJdxTable table, String publicationFields) {
        List<String> res = new ArrayList<>();

        //
        // JdxDbUtils.ID_FIELD пусть будет всегда спереди (необязательно, но... во-первых это красиво!)
        res.add(JdxDbUtils.ID_FIELD);
        if (publicationFields.compareToIgnoreCase("*") == 0) {
            for (IJdxField fieldStruct : table.getFields()) {
                if (fieldStruct.getName().equalsIgnoreCase(JdxDbUtils.ID_FIELD)) {
                    continue;
                }
                res.add(fieldStruct.getName());
            }
        } else {
            String[] publicationFieldsArr = publicationFields.split(",");
            for (String publicationField : publicationFieldsArr) {
                if (publicationField.equalsIgnoreCase(JdxDbUtils.ID_FIELD)) {
                    continue;
                }
                res.add(publicationField);
            }
        }

        //
        return res;
    }

    // todo: переместить отсюда куда-нибудь в утилиты
    public static String filedsToString(Collection<IJdxField> fields) {
        return filedsToString(fields, "");
    }

    public static String filedsToString(Collection<IJdxField> fields, String fieldPrefix) {
        StringBuilder sb = new StringBuilder();

        //
        for (IJdxField f : fields) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(fieldPrefix);
            sb.append(f.getName());
        }

        //
        return sb.toString();
    }


}
