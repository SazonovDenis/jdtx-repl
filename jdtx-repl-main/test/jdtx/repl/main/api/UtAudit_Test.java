package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtAudit_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_LoadRules() throws Exception {
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, struct);

        IJdxDbStruct t = publication.getData();
        for (IJdxTable o : t.getTables()) {
            System.out.println("table = " + o.getName());
            System.out.println("fields = " + o.getFields());
        }
    }


    @Test
    public void test_loadAutitIntervals_0() throws Exception {
        logOn();

        //
        long wsId = 2;
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);

        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, struct);

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
        OutputStream ost = new FileOutputStream("../_test-data/~tmp_csv.xml");
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);
        wr.startDocument();

        // Забираем реплики
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);
        //
        utrr.readAuditData_ById("lic", "id,nameF,nameI,nameO", 0, 10000, wr);
        utrr.readAuditData_ById("usr", "id,name,userName", 0, 10000, wr);
        utrr.readAuditData_ById("region", "id,parent,name", 0, 10000, wr);
        utrr.readAuditData_ById("ulz", "id,region,name", 0, 10000, wr);

        // Закрываем writer
        wr.closeDocument();
    }

    @Test
    public void test_createReplica() throws Exception {
        UtRepl utRepl = new UtRepl(db2, struct2);

        // Делаем изменения
        long wsId = 2;
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct2, wsId);

        // Фиксируем возраст
        long selfAuditAge;
        selfAuditAge = utRepl.getAuditAge();
        System.out.println("curr audit age: " + selfAuditAge);

        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, struct);

        // Формируем реплики
        IReplica replica = utRepl.createReplicaFromAudit(wsId, publication, selfAuditAge);

        //
        System.out.println(replica.getFile().getAbsolutePath());

        // Фиксируем возраст
        selfAuditAge = utRepl.getAuditAge();
        System.out.println("new audit age: " + selfAuditAge);
    }

    @Test
    public void test_applyReplica() throws Exception {
        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, struct);

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/~tmp_csv.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(new FileInputStream(replica.getFile()));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct2);
        utaa.applyReplica(reader, publication, 2, 0);
    }



}
