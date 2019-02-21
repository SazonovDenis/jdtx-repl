package jdtx.repl.main.api;

import org.junit.*;

import java.io.*;
import java.util.*;

public class JdxReplWs_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_ws1_CreateSetupReplica() throws Exception {
        // ---
        // Рабочая станция 1, настройка
        // ---
        JdxReplWs ws1 = new JdxReplWs(db1);
        ws1.init("test/etalon/ws1.json");


        // ---
        // Работаем
        // ---

        // Забираем установочную реплику
        UtRepl utr = new UtRepl(db1);
        utr.dbId = 1;
        for (IPublication publication : ws1.publicationsOut) {
            // Забираем реплику
            IReplica setupReplica = utr.createReplicaFull(publication);

            // Помещаем реплику в очередь
            ws1.queOut.put(setupReplica);
        }

    }

    @Test
    public void test_srv_ApplySetupReplica() throws Exception {
        // ---
        // Рабочая станция, настройка
        // ---
        JdxReplWs ws = new JdxReplWs(db);
        ws.init("test/etalon/ws_srv.json");


        // ---
        // Работаем
        // ---

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
        //
        ws.send();

        // Забираем входящие реплики
        ws.receive();

        //
        ws.pullToQueIn();


        // Применяем входящие реплики
        ws.handleQueIn();

    }

    @Test
    public void test_srv() throws Exception {

        // ---
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db);

        // Пишем в эту очередь
        JdxQueCreatorFile queOut = new JdxQueCreatorFile(db);
        queOut.baseDir = "../_test-data/queOut/";
        queOut.queType = JdxQueType.OUT;
        //
        ws.queOut = queOut;

        // Читаем из этой очереди
        JdxQueCommon queIn = new JdxQueCommon(db);
        queIn.baseDir = "../_test-data/queIn/";
        queIn.queType = JdxQueType.IN;
        //
        ws.queIn = queIn;

        // Правила публикации
        ws.publicationsIn = new ArrayList<>();
        ws.publicationsOut = new ArrayList<>();

        // Загружаем правила публикации
        // todo: доделать инициализацию до уровня реального применения
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r);
            ws.publicationsIn.add(publication);
            ws.publicationsOut.add(publication);
        } finally {
            r.close();
        }


        // ---
        // Проверяем
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);

        //
        long auditAge_0 = ut.getAuditAge();


        // Формируем аудит
        ws.handleSelfAudit();


        //
        long auditAge_1 = ut.getAuditAge();


        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct);


        //
        long auditAge_2 = ut.getAuditAge();

        // Снова формируем аудит
        ws.handleSelfAudit();


        //
        long auditAge_3 = ut.getAuditAge();


        //
        System.out.println(auditAge_0);
        System.out.println(auditAge_1);
        System.out.println(auditAge_2);
        System.out.println(auditAge_3);
    }


    @Test
    public void test_db1() throws Exception {

        // ---
        // Рабочая станция 1, настройка
        JdxReplWs ws = new JdxReplWs(db1);

        // Пишем в эту очередь
        JdxQueCreatorFile queOut = new JdxQueCreatorFile(db1);
        queOut.baseDir = "../_test-data/queOut/";
        queOut.queType = JdxQueType.OUT;
        //
        ws.queOut = queOut;

        // Читаем из этой очереди
        JdxQueCommon queIn = new JdxQueCommon(db1);
        queIn.baseDir = "../_test-data/queIn/";
        queIn.queType = JdxQueType.IN;
        //
        ws.queIn = queIn;

        // Правила публикации
        ws.publicationsIn = new ArrayList<>();
        ws.publicationsOut = new ArrayList<>();

        // Загружаем правила публикации
        // todo: доделать инициализацию до уровня реального применения
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub.json");
        try {
            publication.loadRules(r);
            ws.publicationsIn.add(publication);
            ws.publicationsOut.add(publication);
        } finally {
            r.close();
        }


        // ---
        // Проверяем
        UtAuditAgeManager ut = new UtAuditAgeManager(db1, struct1);

        //
        long auditAge_0 = ut.getAuditAge();


        // Забираем входящие реплики
        ws.pullToQueIn();


        // Применяем входящие реплики
        ws.handleQueIn();


        //
        long auditAge_1 = ut.getAuditAge();


        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct1);


        //
        long auditAge_2 = ut.getAuditAge();

        // Снова читаем чужие реплики
        ws.handleQueIn();


        //
        long auditAge_3 = ut.getAuditAge();


        //
        System.out.println(auditAge_0);
        System.out.println(auditAge_1);
        System.out.println(auditAge_2);
        System.out.println(auditAge_3);
    }


}
