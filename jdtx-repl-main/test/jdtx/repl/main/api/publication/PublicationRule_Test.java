package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
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
        check_PublicationRule(jsonFileName);
    }

    @Test
    public void test_PublicationValid_install_lic_194() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "../install/cfg/publication_lic_194_ws.json";
        check_PublicationRule(jsonFileName);

        //
        jsonFileName = "../install/cfg/publication_lic_194_srv.json";
        check_PublicationRule(jsonFileName);
    }

    @Test
    public void test_PublicationValid_etalon_lic_152() throws Exception {
        System.out.println("Database: " + db.getDbSource().getDatabase());

        //
        String jsonFileName = "test/etalon/publication_lic_152_srv.json";
        check_PublicationRule(jsonFileName);

        //
        jsonFileName = "test/etalon/publication_lic_152_ws.json";
        check_PublicationRule(jsonFileName);
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

    void check_PublicationRule(String jsonFileName) throws Exception {
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


    @Test
    public void test_getDiffRules() throws Exception {
        JSONObject cfgA = UtRepl.loadAndValidateJsonFile("test/jdtx/repl/main/api/publication/publicationA.json");
        JSONObject cfgB = UtRepl.loadAndValidateJsonFile("test/jdtx/repl/main/api/publication/publicationB.json");

        //
        IPublicationRuleStorage publicationOutA = PublicationRuleStorage.loadRules(cfgA, struct, "out");
        IPublicationRuleStorage publicationOutB = PublicationRuleStorage.loadRules(cfgB, struct, "out");
        IPublicationRuleStorage publicationInA = PublicationRuleStorage.loadRules(cfgA, struct, "in");
        IPublicationRuleStorage publicationInB = PublicationRuleStorage.loadRules(cfgB, struct, "in");

        List<IJdxTable> tablesAddedOut = new ArrayList<>();
        List<IJdxTable> tablesRemovedOut = new ArrayList<>();
        List<IJdxTable> tablesChangedOut = new ArrayList<>();
        UtPublicationRule.getPublicationRulesDiff(struct, publicationOutA, publicationOutB, tablesAddedOut, tablesRemovedOut, tablesChangedOut);

        List<IJdxTable> tablesAddedIn = new ArrayList<>();
        List<IJdxTable> tablesRemovedIn = new ArrayList<>();
        List<IJdxTable> tablesChangedIn = new ArrayList<>();
        UtPublicationRule.getPublicationRulesDiff(struct, publicationInA, publicationInB, tablesAddedIn, tablesRemovedIn, tablesChangedIn);

        // Печатаем
        System.out.println("Added in B");
        for (IJdxTable table : tablesAddedOut) {
            System.out.println("  " + table.getName());
        }
        System.out.println("Removed in B");
        for (IJdxTable table : tablesRemovedOut) {
            System.out.println("  " + table.getName());
        }
        System.out.println("Changed in B");
        for (IJdxTable table : tablesChangedOut) {
            System.out.println("  " + table.getName());
        }

        assertEquals(1, tablesAddedOut.size());
        assertEquals(2, tablesRemovedOut.size());
        assertEquals(1, tablesChangedOut.size());
        //
        assertEquals(0, tablesAddedIn.size());
        assertEquals(0, tablesRemovedIn.size());
        assertEquals(0, tablesChangedIn.size());
    }


    /**
     *
     */
    @Test
    public void test_createCfgSnapshot() throws Exception {
        CfgManager cfgManager = new CfgManager(db);
        //JSONObject cfg_ws = cfgManager.getWsCfg(CfgType.PUBLICATIONS, 3);

        JSONObject cfg_file_lic = UtRepl.loadAndValidateJsonFile("test/etalon/publication_lic_152_ws.json");
        JSONObject cfg_file_full = UtRepl.loadAndValidateJsonFile("test/etalon/publication_full_152_ws.json");

        //
        System.out.println("=== cfg_file_full");
        cfgManager.setWsCfg(cfg_file_full, CfgType.PUBLICATIONS, 3);
        createCfgSnapshot(3);

        //
        System.out.println();
        System.out.println("=== cfg_file_lic");
        cfgManager.setWsCfg(cfg_file_lic, CfgType.PUBLICATIONS, 3);
        createCfgSnapshot(3);
    }

    void createCfgSnapshot(long wsId) throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        IPublicationRuleStorage ruleSnapshot = srv.createCfgSnapshot(wsId);

        //
        printPublicationRules(ruleSnapshot.getPublicationRules());
    }

}