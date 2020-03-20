package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.test.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 * Проверяем формирование и прием Snapshot-реплик
 */
public class UtData_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_createReplicaTableSnapshot() throws Exception {
        // Готовим реплику от ws2
        IReplica replica = createReplicaSnapshot_Ulz_ws2();

        // Копируем реплику для анализа
        File f = new File("../_test-data/ws_002/tmp/000000001-src.zip");
        FileUtils.copyFile(replica.getFile(), f);

        //
        System.out.println("replica: " + f);
        System.out.println("replica.wsId = " + replica.getInfo().getWsId());
        System.out.println("replica.age = " + replica.getInfo().getAge());
        System.out.println("replica.replicaType = " + replica.getInfo().getReplicaType());
    }


    @Test
    public void test_applyReplicaSnapshot() throws Exception {
        StopWatch sw = new StopWatch();

        // Готовим реплику от ws2
        sw.start("Готовим реплику");
        //
        IReplica replicaSnapshot = createReplicaTableSnapshot_ws2("pawnchitopr");
        //
        sw.stop();

        // Читатель реплики
        InputStream inputStream = UtRepl.getReplicaInputStream(replicaSnapshot);
        JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

        // Правила публикации
        JSONObject cfgPublication = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_166.json"));
        String publicationName = (String) cfgPublication.get("in");
        JSONObject cfgPublicationIn = (JSONObject) cfgPublication.get(publicationName);
        //
        IPublication publication = new Publication();
        publication.loadRules(cfgPublicationIn, struct);

        // Применяем реплику на ws1
        sw.start("Применяем реплику");

        // Стратегии перекодировки каждой таблицы
        JSONObject cfgDbDecode = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/decode_strategy.json"));
        RefDecodeStrategy.instance = new RefDecodeStrategy();
        RefDecodeStrategy.instance.init(cfgDbDecode);

        // Применятель
        UtAuditApplyer auditApplyer = new UtAuditApplyer(db, struct);
        long wsId = 1;
        auditApplyer.applyReplica(replicaReader, publication, false, wsId, 0);
        //
        sw.stop();

        //
        inputStream.close();
    }


    public IReplica createReplicaSnapshot_Ulz_ws2() throws Exception {
        return createReplicaTableSnapshot_ws2("ulz");
    }

    public IReplica createReplicaTableSnapshot_ws2(String tableName) throws Exception {

        // Правила публикации
        JSONObject cfgPublication = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_166.json"));
        String publicationName = (String) cfgPublication.get("out");
        JSONObject cfgPublicationOut = (JSONObject) cfgPublication.get(publicationName);
        //
        IPublication publication = new Publication();
        publication.loadRules(cfgPublicationOut, struct2);

        // Забираем установочную реплику
        long wsId = 2;
        UtRepl utRepl = new UtRepl(db2, struct2);
        IReplica replica = utRepl.createReplicaTableSnapshot(wsId, publication.getData().getTable(tableName), 1);

        //
        return replica;
    }


}
