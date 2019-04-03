package jdtx.repl.main.api;

import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtAudit_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_LoadRules() throws Exception {
        Publication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
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
        UtRepl utRepl = new UtRepl(db, struct);

        // Проверяем возраст
        long auditAge_A_0 = utRepl.markAuditAge();
        long auditAge_A_1 = utRepl.getAuditAge();
        long auditAge_A_2 = utRepl.markAuditAge();
        long auditAge_A_3 = utRepl.markAuditAge();
        long auditAge_A_4 = utRepl.getAuditAge();

        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct, 1);
        utTest.makeChange(struct, 1);

        //
        long auditAge_A_5 = utRepl.getAuditAge();

        //
        utTest.makeChange(struct, 1);
        utTest.makeChange(struct, 1);

        //
        long auditAge_A_6 = utRepl.getAuditAge();


        // Фиксируем возраст
        long auditAge_B_0 = utRepl.markAuditAge();
        long auditAge_B_1 = utRepl.getAuditAge();
        long auditAge_B_2 = utRepl.markAuditAge();
        long auditAge_B_3 = utRepl.markAuditAge();
        long auditAge_B_4 = utRepl.getAuditAge();


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
        UtRepl utRepl = new UtRepl(db2, struct2);
        long wsId = 2;

        // Делаем изменения
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct, wsId);

        // Фиксируем возраст
        long selfAuditAge = utRepl.markAuditAge();
        System.out.println("selfAuditAge = " + selfAuditAge);

        // Готовим writer
        OutputStream ost = new FileOutputStream("../_test-data/~tmp_csv.xml");
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);
        //
        wr.writeReplicaInfo(wsId, 1, JdxReplicaType.IDE);

        // Забираем реплики
        UtAuditSelector utrr = new UtAuditSelector(db2, struct, wsId);
        //
        utrr.readAuditData("lic", "id,nameF,nameI,nameO", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("usr", "id,name,userName", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("region", "id,parent,name", selfAuditAge, selfAuditAge, wr);
        utrr.readAuditData("ulz", "id,region,name", selfAuditAge, selfAuditAge, wr);
        //
        wr.closeDocument();
    }

    @Test
    public void test_createSetupReplica() throws Exception {
        //logOn();

        UtRepl utRepl = new UtRepl(db, struct);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Увеличиваем возраст
        long age = utRepl.incAuditAge();
        System.out.println("new AuditAge = " + age);

        // Забираем установочную реплику
        IReplica replica = utRepl.createReplicaSnapshot(1, publication, age);

        //
        System.out.println(replica.getFile().getAbsolutePath());
    }

    @Test
    public void test_createSetupReplica_full() throws Exception {
        //logOn();

        UtRepl utRepl = new UtRepl(db, struct);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/publication_full_152.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Увеличиваем возраст
        long age = utRepl.incAuditAge();
        System.out.println("new AuditAge = " + age);

        // Забираем установочную реплику
        IReplica replica = utRepl.createReplicaSnapshot(1, publication, age);

        //
        System.out.println(replica.getFile().getAbsolutePath());
    }

    @Test
    public void test_createReplica() throws Exception {
        UtRepl utRepl = new UtRepl(db, struct);

        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct, 1);

        // Фиксируем возраст
        long selfAuditAge = utRepl.markAuditAge();
        System.out.println("new AuditAge = " + selfAuditAge);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Формируем реплики
        IReplica replica = utRepl.createReplicaFromAudit(1, publication, selfAuditAge);

        //
        System.out.println(replica.getFile().getAbsolutePath());
    }

    @Test
    public void test_applyReplica() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/~tmp_csv.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(new FileInputStream(replica.getFile()));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct);
        utaa.applyReplica(reader, publication, 2);
    }

    @Test
    public void test_applyReplicaSnapshot() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/csv_full.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(new FileInputStream(replica.getFile()));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct);
        utaa.applyReplica(reader, publication, 2);
    }

    @Test
    public void test_big_apply() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/publication_full_152.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/000000001-big.zip"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(new FileInputStream(replica.getFile()));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct);
        utaa.applyReplica(reader, publication, 2);
    }

}
