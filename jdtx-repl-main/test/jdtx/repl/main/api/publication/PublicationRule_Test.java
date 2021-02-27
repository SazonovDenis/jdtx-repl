package jdtx.repl.main.api.publication;

import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class PublicationRule_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/srv/";

        super.setUp();
    }

    @Test
    public void test_LoadRules_1() throws Exception {
        System.out.println("Publication: pub.json/out");
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        IPublicationStorage publicationOut = PublicationStorage.extractPublicationRules(cfg, struct, "out");
        printPublicationRules(publicationOut.getPublicationRules());
    }

    @Test
    public void test_LoadRules_2() throws Exception {
        System.out.println("Publication: publication_lic.json/out");
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/jdtx/repl/main/api/publication/publication_lic.json"));
        IPublicationStorage publicationOut = PublicationStorage.extractPublicationRules(cfg, struct, "out");
        printPublicationRules(publicationOut.getPublicationRules());
    }

    private void printPublicationRules(Collection<IPublicationRule> rules) {
        for (IPublicationRule rule : rules) {
            System.out.println("table: " + rule.getTableName());
            System.out.println("  fields: " + JdxUtils.fieldsToString(rule.getFields()));
            System.out.println("  filter: " + rule.getFilterExpression());
        }
    }

    @Test
    public void test_PublicationValidFk() throws Exception {
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/jdtx/repl/main/api/publication/publication_lic.json"));

        //
        System.out.println("Publication: out");
        IPublicationStorage publicationOut = PublicationStorage.extractPublicationRules(cfg, struct, "out");
        //UtPublicationRule.checkValidRef(publicationOut, struct);

        //
        System.out.println("Publication: in");
        IPublicationStorage publicationIn = PublicationStorage.extractPublicationRules(cfg, struct, "in");
        //UtPublicationRule.checkValidRef(publicationIn, struct);
    }


}
