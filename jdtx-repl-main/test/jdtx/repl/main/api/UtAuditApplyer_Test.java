package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtAuditApplyer_Test extends DbmTestCase {

    IJdxDbStruct struct;

    public void setUp() throws Exception {
        super.setUp();

        // Утилиты
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
        struct = reader.readDbStruct();
    }

    @Test
    public void test_apply() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());

        // Узнаем возраст
        long age = utr.getAuditAge();
        System.out.println("age = " + age);

        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("temp/pub.json");
        try {
            publcation.load(r);
        } finally {
            r.close();
        }

        // Реплики
        IReplica replica = new Replica();
        replica.setFile(new File("temp/csv.xml"));

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(dbm.getDb(), struct);
        utaa.applyAuditData(replica, publcation, null);
    }

}
