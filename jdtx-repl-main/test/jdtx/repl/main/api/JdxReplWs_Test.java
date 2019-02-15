package jdtx.repl.main.api;

import org.junit.*;

import java.io.*;
import java.util.*;

public class JdxReplWs_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_db() throws Exception {

        // ---
        // Рабочая станция, настройка
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
        // Рабочая станция, настройка
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


        // Читаем чужие реплики
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
