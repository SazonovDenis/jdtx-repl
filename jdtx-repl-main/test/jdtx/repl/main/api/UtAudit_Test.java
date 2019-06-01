package jdtx.repl.main.api;

import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
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
            publication.loadRules(r, struct);
        } finally {
            r.close();
        }

        IJdxDbStruct t = publication.getData();

        for (IJdxTableStruct o : t.getTables()) {
            System.out.println("table = " + o.getName());
            System.out.println("fields = " + o.getFields());
        }

    }

    @Test
    public void test_1() throws Exception {
        long wsId = 2;
        UtAuditSelector utrr = new UtAuditSelector(db2, struct2, wsId);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/publication_full.json");
        //Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r, struct);
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
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r, struct2);
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
            publication.loadRules(r, struct2);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/~tmp_csv.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(new FileInputStream(replica.getFile()));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct2);
        utaa.applyReplica(reader, publication, 2);
    }

    @Test
    public void test_applyReplicaSnapshot() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r, struct2);
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


}
