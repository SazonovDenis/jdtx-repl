package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtRepl_RunnerSample_Test extends ReplDatabase_Test {


    IJdxDbStruct struct;

    //
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    JdxQueCreator queIn;
    JdxQueCreator queOut;


    public void setUp() throws Exception {
        super.setUp();

        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();


        // Правила публикации
        publicationsIn = new ArrayList<>();
        publicationsOut = new ArrayList<>();

        // Загружаем правила публикации     todo: доделать инициализацию до уровня реального применения
        IPublication publication = new Publication();
        Reader r = new FileReader("../_test-data/etalon/pub.json");
        try {
            publication.loadRules(r);
            publicationsIn.add(publication);
            publicationsOut.add(publication);
        } finally {
            r.close();
        }

    }

    @Test
    public void sample_runSrv() throws Exception {
        SrvQueManager srvQueManager = new SrvQueManager();
        UtRepl utr = new UtRepl(db1);

        // Отслеживаем и обрабатываем свои изменения
        utr.queOut = srvQueManager.localOutQue;
        utr.handleSelfAudit();

        // Обрабатыываем очереди на сервере
        srvQueManager.handleQue();

        // Применяем входящие реплики
        utr.handleInAudit();
    }


    @Test
    public void sample_runWs() throws Exception {
        UtRepl utr = new UtRepl(db);

        //
        utr.queIn = queIn;
        utr.queOut = queOut;


        // Отслеживаем и обрабатываем свои изменения
        utr.handleSelfAudit();

        // Применяем входящие реплики
        utr.handleInAudit();
    }


}
