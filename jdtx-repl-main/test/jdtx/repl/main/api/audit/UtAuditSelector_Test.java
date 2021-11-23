package jdtx.repl.main.api.audit;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtAuditSelector_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_loadAutitIntervals_0() throws Exception {
        logOn();

        //
        long wsId = 2;
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);

        // Загружаем правила публикации
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/publication_full_152.json");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, struct, "in");

        //
        long age;
        Map m;

        // ---
        age = 1;

        //
        m = utrr.loadAutitIntervals(publication, age);

        //
        System.out.println("age: " + age);
        System.out.println(m);
        System.out.println("z_opr_dttm_from: " + m.get("z_opr_dttm_from"));
        System.out.println("z_opr_dttm_to  : " + m.get("z_opr_dttm_to"));
        System.out.println("");

        // ---
        age = 8;

        //
        m = utrr.loadAutitIntervals(publication, age);

        //
        System.out.println("age: " + age);
        System.out.println(m);
        System.out.println("z_opr_dttm_from: " + m.get("z_opr_dttm_from"));
        System.out.println("z_opr_dttm_to  : " + m.get("z_opr_dttm_to"));
        System.out.println("");
    }

    @Test
    public void test_readAuditData() throws Exception {
        long wsId = 2;

        // Делаем изменения
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct, wsId);

        // Готовим writer
        OutputStream ost = new FileOutputStream("temp/~tmp_csv.xml");
        JdxReplicaWriterXml writerXml = new JdxReplicaWriterXml(ost);
        //
        writerXml.startDocument();

        // Забираем реплики
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);
        //
        utrr.readAuditData_ByInterval("lic", "id,nameF,nameI,nameO", 0, 10000, writerXml);
        utrr.readAuditData_ByInterval("usr", "id,name,userName", 0, 10000, writerXml);
        utrr.readAuditData_ByInterval("region", "id,parent,name", 0, 10000, writerXml);
        utrr.readAuditData_ByInterval("ulz", "id,region,name", 0, 10000, writerXml);

        // Закрываем writer
        writerXml.closeDocument();
        //
        writerXml.close();
    }

    @Test
    public void test_createReplica() throws Exception {
        // Делаем изменения
        long wsId = 2;
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct2, wsId);

        // Фиксируем возраст
        long selfAuditAge;
        UtAuditAgeManager ut = new UtAuditAgeManager(db2, struct2);
        selfAuditAge = ut.getAuditAge();
        System.out.println("curr audit age: " + selfAuditAge);

        // Загружаем правила публикации
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/pub.json");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, struct2, "in");

        // Формируем реплики
        UtAuditSelector auditSelector = new UtAuditSelector(db2, struct2, wsId);
        IReplica replica = auditSelector.createReplicaFromAudit(publication, selfAuditAge);

        //
        System.out.println(replica.getData().getAbsolutePath());

        // Фиксируем возраст
        selfAuditAge = ut.getAuditAge();
        System.out.println("new audit age: " + selfAuditAge);
    }


}
