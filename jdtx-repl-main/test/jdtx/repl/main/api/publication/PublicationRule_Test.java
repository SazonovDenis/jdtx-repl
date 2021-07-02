package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.*;
import org.json.simple.*;
import org.junit.*;

import java.util.*;

/**
 * Проверяет согласованность и непротиворечивость (по ссылкам)
 * конфигураций для фильтрации.
 */
public class PublicationRule_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_LoadRules_1() throws Exception {
        System.out.println("Publication: pub.json/out");
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/pub.json");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfg, struct, "out");
        printPublicationRules(publicationOut.getPublicationRules());
    }

    @Test
    public void test_LoadRules_2() throws Exception {
        System.out.println("Publication: publication_lic.json/out");
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/jdtx/repl/main/api/publication/publication_lic.json");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfg, struct, "out");
        printPublicationRules(publicationOut.getPublicationRules());
    }

    private void printPublicationRules(Collection<IPublicationRule> rules) {
        for (IPublicationRule rule : rules) {
            System.out.println("table: " + rule.getTableName());
            System.out.println("  fields: " + UtJdx.fieldsToString(rule.getFields()));
            System.out.println("  filter: " + rule.getFilterExpression());
        }
    }

    @Test
    public void test_PublicationValid_test_lic() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "test/jdtx/repl/main/api/publication/publication_lic.json";
        test_PublicationRule(jsonFileName);
    }

    @Test
    public void test_PublicationValid_install_lic_194() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "../install/cfg/publication_lic_194_ws.json";
        test_PublicationRule(jsonFileName);

        //
        jsonFileName = "../install/cfg/publication_lic_194_srv.json";
        test_PublicationRule(jsonFileName);
    }

    @Test
    public void test_PublicationValid_etalon_lic_152() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "test/etalon/publication_lic_152_srv.json";
        test_PublicationRule(jsonFileName);

        //
        jsonFileName = "test/etalon/publication_lic_152_ws.json";
        test_PublicationRule(jsonFileName);
    }

    @Test
    public void test_PublicationValid_install_lic_194_snapshot() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "../install/cfg/publication_lic_194_snapshot.json";

        //
        System.out.println("Json file: " + jsonFileName);
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(jsonFileName);

        //
        System.out.println("Publication: snapshot");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, struct, "snapshot");
        JSONObject cfgPublication = PublicationRuleStorage.extractRulesByName(cfg, "snapshot");
        UtPublicationRule.checkValid(cfgPublication, publication, struct);
    }

    @Test
    public void test_PublicationValid_full_152_snapshot() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "test/etalon/publication_full_152_snapshot.json";

        //
        System.out.println("Json file: " + jsonFileName);
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(jsonFileName);

        //
        System.out.println("Publication: snapshot");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, struct, "snapshot");
        JSONObject cfgPublication = PublicationRuleStorage.extractRulesByName(cfg, "snapshot");
        UtPublicationRule.checkValid(cfgPublication, publication, struct);
    }

    @Test
    public void test_PublicationValid_lic_152_snapshot() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "test/etalon/publication_lic_152_snapshot.json";

        //
        System.out.println("Json file: " + jsonFileName);
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(jsonFileName);

        //
        System.out.println("Publication: snapshot");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, struct, "snapshot");
        JSONObject cfgPublication = PublicationRuleStorage.extractRulesByName(cfg, "snapshot");
        UtPublicationRule.checkValid(cfgPublication, publication, struct);
    }

    void test_PublicationRule(String jsonFileName) throws Exception {
        System.out.println("Json file: " + jsonFileName);
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(jsonFileName);

        //
        System.out.println("Publication: in");
        IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfg, struct, "in");
        JSONObject cfgPublicationRulesIn = PublicationRuleStorage.extractRulesByName(cfg, "in");
        UtPublicationRule.checkValid(cfgPublicationRulesIn, publicationIn, struct);

        //
        System.out.println("Publication: out");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfg, struct, "out");
        JSONObject cfgPublicationRulesOut = PublicationRuleStorage.extractRulesByName(cfg, "out");
        UtPublicationRule.checkValid(cfgPublicationRulesOut, publicationOut, struct);
    }


}
