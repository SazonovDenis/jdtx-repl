package jdtx.repl.main.api.publication;

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
    public void loadRules(JSONObject cfg, IJdxDbStruct struct) throws Exception {
        publicationRules.clear();

        for (Object key : cfg.keySet()) {
            JSONObject publicationRuleJson = (JSONObject) cfg.get(key);
            String publicationTableName = (String) key;

            //
            IJdxTable structTable = struct.getTable(publicationTableName);
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
            List<String> publicationFields = UtPublicationRule.expandPublicationFields(structTable, (String) publicationRuleJson.get("fields"));
            for (String publicationFieldName : publicationFields) {
                IJdxField publicationField = structTable.getField(publicationFieldName).cloneField();
                publicationRule.getFields().add(publicationField);
            }

            // IPublicationRule.setFilterExpression
            publicationRule.setFilterExpression((String) publicationRuleJson.getOrDefault("filter", null));
        }

        //
        UtPublicationRule.checkValidRef(this, struct);
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

    /**
     * Из json-объекта cfgPublications создает правила публикации по имени publicationName (обычно "in" или "out")
     */
    public static IPublicationStorage extractPublicationRules(JSONObject cfgPublications, IJdxDbStruct structActual, String publicationName) throws Exception {
        IPublicationStorage publicationRules = new PublicationStorage();
        //
        if (cfgPublications != null) {
            String publicationRuleName = (String) cfgPublications.get(publicationName);
            JSONObject cfgPublicationRule = (JSONObject) cfgPublications.get(publicationRuleName);
            publicationRules.loadRules(cfgPublicationRule, structActual);
        }
        //
        return publicationRules;
    }


}
