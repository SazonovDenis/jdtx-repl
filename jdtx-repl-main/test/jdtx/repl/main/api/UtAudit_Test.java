package jdtx.repl.main.api;

import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

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

        JSONArray t = publication.getTables();

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
/*
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
*/
    @Test
    public void test_1() throws Exception {
        long wsId = 2;
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/publication_full.json");
        //Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        //
        Map m = utrr.loadAutitIntervals(publication, 8);

        //
        System.out.println(m);
        System.out.println("z_opr_dttm_from: " + m.get("z_opr_dttm_from"));
        System.out.println("z_opr_dttm_to  : " + m.get("z_opr_dttm_to"));
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
        UtAuditSelector utrr = new UtAuditSelector(db2, struct, wsId);
        //
        utrr.readAuditData_ById("lic", "id,nameF,nameI,nameO", 0, 10000, wr);
        utrr.readAuditData_ById("usr", "id,name,userName", 0, 10000, wr);
        utrr.readAuditData_ById("region", "id,parent,name", 0, 10000, wr);
        utrr.readAuditData_ById("ulz", "id,region,name", 0, 10000, wr);

        // Закрываем writer
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
        UtRepl utRepl = new UtRepl(db2, struct2);

        // Делаем изменения
        long wsId = 2;
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct, wsId);

        // Фиксируем возраст
        long selfAuditAge;
        selfAuditAge = utRepl.getAuditAge();
        System.out.println("curr audit age: " + selfAuditAge);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

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
