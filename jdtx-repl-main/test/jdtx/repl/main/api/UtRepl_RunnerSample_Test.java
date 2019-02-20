package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 * todo: Нужен ли ? Мусор походу - УДАЛИТЬ
 */
public class UtRepl_RunnerSample_Test extends ReplDatabase_Test {


    IJdxDbStruct struct;

    //
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    JdxQueCreatorFile queIn;
    JdxQueCreatorFile queOut;


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
        Reader r = new FileReader("test/etalon/pub.json");
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

/*
        // Отслеживаем и обрабатываем свои изменения
        utr.queOut = srvQueManager.localOutQue;
        utr.handleSelfAudit();

        // Обрабатыываем очереди на сервере
        srvQueManager.handleQue();

        // Применяем входящие реплики
        utr.handleInQue();
*/
    }


    @Test
    public void sample_runWs() throws Exception {
        JdxReplWs replWs = new JdxReplWs(db);

        //
        replWs.queIn = queIn;
        replWs.queOut = queOut;


        // Отслеживаем и обрабатываем свои изменения
        replWs.handleSelfAudit();


        // Забираем входящие реплики
        replWs.pullToQueIn();


        // Применяем входящие реплики
        replWs.handleInQue();
    }


}
