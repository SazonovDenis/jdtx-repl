package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtRepl_AuditReader_Test extends DbmTestCase {

    UtTest utTest;
    IJdxDbStruct struct;

    public void setUp() throws Exception {
        super.setUp();

        // Утилиты
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
        struct = reader.readDbStruct();
        //
        this.utTest = new UtTest(dbm.getDb(), struct);
    }

    @Test
    public void test_Publication() throws Exception {
        Publication p = new Publication();
        Reader r = new FileReader("temp/pub.json");
        try {
            p.load(r);
        } finally {
            r.close();
        }

        JSONArray t = p.getData();

        for (int i = 0; i < t.size(); i++) {
            JSONObject o = (JSONObject) t.get(i);
            System.out.println("table=" + o.get("table"));
            System.out.println("fields=" + o.get("fields"));
        }

    }

    @Test
    public void test_select() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());

        // Делаем изменения
        utTest.makeChange();

        // Фиксируем возраст
        long age = utr.markAuditAge();
        System.out.println("age = " + age);

        // Забираем реплики
        UtAuditSelector utrr = new UtAuditSelector(dbm.getDb(), struct);

        OutputStream ost = new FileOutputStream("temp/csv.xml");
        JdxDataWriter wr = new JdxDataWriter(ost);
        //
        utrr.fillAuditData("lic", "*", age, age, wr);
        utrr.fillAuditData("usr", "*", age, age, wr);
        utrr.fillAuditData("region", "*", age, age, wr);
        utrr.fillAuditData("ulz", "*", age, age, wr);
        //
        wr.close();
    }

    @Test
    public void test_createReplica() throws Exception {
        //logOn();

        UtRepl utr = new UtRepl(dbm.getDb());

        // Делаем изменения
        utTest.makeChange();

        // Фиксируем возраст
        long age = utr.markAuditAge();
        System.out.println("age = " + age);

        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("temp/pub.json");
        try {
            publcation.load(r);
        } finally {
            r.close();
        }

        // Забираем реплики
        IReplica replica = utr.createReplica(publcation, 0, age);

        //
        System.out.println(replica.getFile().getAbsolutePath());

    }

}
