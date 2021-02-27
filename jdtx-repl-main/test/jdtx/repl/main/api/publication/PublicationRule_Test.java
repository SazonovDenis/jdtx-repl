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
    public void test_LoadRules() throws Exception {
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        //
        String publicationName = (String) cfg.get("out");
        JSONObject cfgPublicationOut = (JSONObject) cfg.get(publicationName);
        //
        IPublicationStorage publication = new PublicationStorage();
        publication.loadRules(cfgPublicationOut, struct);

        //
        Collection<IPublicationRule> t = publication.getPublicationRules();
        for (IPublicationRule rule : t) {
            System.out.println("table = " + rule.getTableName());
            System.out.println("fields = " + rule.getFields());
            System.out.println("authorWs = " + rule.getAuthorWs());
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
