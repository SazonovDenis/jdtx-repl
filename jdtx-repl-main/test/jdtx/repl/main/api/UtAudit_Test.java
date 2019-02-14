package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtAudit_Test extends ReplDatabase_Test {

    IJdxDbStruct struct;
    IJdxDbStruct struct1;

    public void setUp() throws Exception {
        super.setUp();

        // Утилиты
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
        reader.setDb(db1);
        struct1 = reader.readDbStruct();
    }

    @Test
    public void test_LoadRules() throws Exception {
        Publication publication = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        JSONArray t = publication.getData();

        for (int i = 0; i < t.size(); i++) {
            JSONObject o = (JSONObject) t.get(i);
            System.out.println("table=" + o.get("table"));
            System.out.println("fields=" + o.get("fields"));
        }

    }

    @Test
    public void test_readAuditData() throws Exception {
        UtRepl utr = new UtRepl(db);

        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct);

        // Фиксируем возраст
        long selfAuditAge = utr.markAuditAge();
        System.out.println("selfAuditAge = " + selfAuditAge);

        // Забираем реплики
        UtAuditSelector utrr = new UtAuditSelector(db, struct);

        OutputStream ost = new FileOutputStream("../_test-data/csv.xml");
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);
        //
        utrr.readAuditData("lic", "*", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("usr", "*", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("region", "*", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("ulz", "*", selfAuditAge, selfAuditAge, wr);
        //
        wr.close();
    }

    @Test
    public void test_createSetupReplica() throws Exception {
        //logOn();

        UtRepl utr = new UtRepl(db);

        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub_full.json");
        try {
            publcation.loadRules(r);
        } finally {
            r.close();
        }

        // Забираем установочную реплику
        IReplica replica = utr.createReplicaFull(publcation);

        //
        System.out.println(replica.getFile().getAbsolutePath());
    }

    @Test
    public void test_createReplica() throws Exception {
        //logOn();

        UtRepl utr = new UtRepl(db);

        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct);

        // Фиксируем возраст
        long selfAuditAge = utr.markAuditAge();
        System.out.println("selfAuditAge = " + selfAuditAge);

        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub.json");
        try {
            publcation.loadRules(r);
        } finally {
            r.close();
        }

        // Забираем реплики
        IReplica replica = utr.createReplicaFromAudit(publcation, 0, selfAuditAge);

        //
        System.out.println(replica.getFile().getAbsolutePath());
    }

    @Test
    public void test_applyReplica() throws Exception {
        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub.json");
        try {
            publcation.loadRules(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new Replica();
        replica.setFile(new File("../_test-data/csv.xml"));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db1, struct);
        utaa.applyReplica(replica, publcation, null);
    }

    @Test
    public void test_applyReplicaFull() throws Exception {
        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub_full.json");
        try {
            publcation.loadRules(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new Replica();
        replica.setFile(new File("../_test-data/csv_full.xml"));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db1, struct);
        utaa.applyReplica(replica, publcation, null);
    }

}
