package jdtx.repl.main.api;

import org.junit.*;

import java.io.*;
import java.util.*;

public class JdxReplWs_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_doSetupReplica() throws Exception {
        // ---
        // Рабочая станция 1, настройка
        // ---
        JdxReplWs ws1 = new JdxReplWs(db1);

        // Пишем в эту очередь
        ws1.queOut = new JdxQueCreatorFile(db1);
        ws1.queOut.baseFilePath = "../_test-data/queOut/ws1";
        ws1.queOut.queType = JdxQueType.OUT;
        // ---


        // ---
        // Рабочая станция 2, настройка
        // ---
        JdxReplWs ws2 = new JdxReplWs(db2);

        // Пишем в эту очередь
        ws2.queOut = new JdxQueCreatorFile(db2);
        ws2.queOut.baseFilePath = "../_test-data/queOut/ws2";
        ws2.queOut.queType = JdxQueType.OUT;
        // ---


        // ---
        // Загружаем правила публикации
        // ---
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }
        // ---


        // ---
        // Работаем
        // ---

        // Забираем установочную реплику
        UtRepl utr1 = new UtRepl(db1);
        IReplica setupReplica = utr1.createReplicaFull(publication);

        // Помещаем установочную реплику в очередь
        ws1.queOut.put(setupReplica);


        // Забираем установочную реплику
        UtRepl utr2 = new UtRepl(db2);
        IReplica setupReplica2 = utr2.createReplicaFull(publication);

        // Помещаем установочную реплику в очередь
        ws2.queOut.put(setupReplica2);
    }

    @Test
    public void test_srv() throws Exception {

        // ---
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db);

        // Пишем в эту очередь
        JdxQueCreatorFile queOut = new JdxQueCreatorFile(db);
        queOut.baseFilePath = "../_test-data/queOut/";
        queOut.queType = JdxQueType.OUT;
        //
        ws.queOut = queOut;

        // Читаем из этой очереди
        JdxQueCreatorFile queIn = new JdxQueCreatorFile(db);
        queIn.baseFilePath = "../_test-data/queIn/";
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
        queOut.baseFilePath = "../_test-data/queOut/";
        queOut.queType = JdxQueType.OUT;
        //
        ws.queOut = queOut;

        // Читаем из этой очереди
        JdxQueCreatorFile queIn = new JdxQueCreatorFile(db1);
        queIn.baseFilePath = "../_test-data/queIn/";
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
        ws.handleInQue();


        //
        long auditAge_1 = ut.getAuditAge();


        // Делаем изменения
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct1);


        //
        long auditAge_2 = ut.getAuditAge();

        // Снова читаем чужие реплики
        ws.handleInQue();


        //
        long auditAge_3 = ut.getAuditAge();


        //
        System.out.println(auditAge_0);
        System.out.println(auditAge_1);
        System.out.println(auditAge_2);
        System.out.println(auditAge_3);
    }


}
