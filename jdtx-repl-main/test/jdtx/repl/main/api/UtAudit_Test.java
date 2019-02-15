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

    /**
     * Проверим оптимизацию лишних фиксаций:
     * если не было реальных изменений - возраст не меняется
     */
    @Test
    public void test_markAuditAge() throws Exception {
        UtRepl utr = new UtRepl(db);

        // Проверяем возраст
        long auditAge_A_0 = utr.markAuditAge();
        long auditAge_A_1 = utr.getAuditAge();
        long auditAge_A_2 = utr.markAuditAge();
        long auditAge_A_3 = utr.markAuditAge();
        long auditAge_A_4 = utr.getAuditAge();

        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct);
        utTest.makeChange(struct);

        //
        long auditAge_A_5 = utr.getAuditAge();

        //
        utTest.makeChange(struct);
        utTest.makeChange(struct);

        //
        long auditAge_A_6 = utr.getAuditAge();


        // Фиксируем возраст
        long auditAge_B_0 = utr.markAuditAge();
        long auditAge_B_1 = utr.getAuditAge();
        long auditAge_B_2 = utr.markAuditAge();
        long auditAge_B_3 = utr.markAuditAge();
        long auditAge_B_4 = utr.getAuditAge();


        //
        System.out.println("auditAge_A = " + auditAge_A_0);
        System.out.println("auditAge_B = " + auditAge_B_0);

        //
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_1);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_2);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_3);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_4);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_5);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_A_6);
        assertEquals("Возраст аудита", auditAge_A_0, auditAge_B_0 - 1);
        assertEquals("Возраст аудита", auditAge_B_0, auditAge_B_1);
        assertEquals("Возраст аудита", auditAge_B_0, auditAge_B_2);
        assertEquals("Возраст аудита", auditAge_B_0, auditAge_B_3);
        assertEquals("Возраст аудита", auditAge_B_0, auditAge_B_4);
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
