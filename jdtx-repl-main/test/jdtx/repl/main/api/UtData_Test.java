package jdtx.repl.main.api;

import jandcode.dbm.data.*;
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
import java.util.*;

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
        IReplica replicaSnapshot = createReplicaTableSnapshot_ws2("PawnChitOpr");
        //
        sw.stop();

        // Читатель реплики
        InputStream inputStream = JdxReplicaReaderXml.createInputStreamData(replicaSnapshot);
        JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

        // Правила публикации
        JSONObject cfgPublication = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_166.json"));
        String publicationName = (String) cfgPublication.get("in");
        JSONObject cfgPublicationIn = (JSONObject) cfgPublication.get(publicationName);
        //
        IPublicationStorage publication = new PublicationStorage();
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


    @Test
    public void test_JdxReplicaReader() throws Exception {
        // Создаем репликацию (ws1, ws2, ws3) через prepareEtalon_TestAll

        // Вот что у нас есть базе!
        UtData.outTable(db2.loadSql("select id, ChitNo, ChitDt from PawnChit order by id"));
        UtData.outTable(db2.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        // Готовим реплику от ws2
        IReplica replicaSnapshotUlz = createReplicaSnapshot_Ulz_ws2();
        IReplica replicaSnapshotLic = createReplicaSnapshot_Lic_ws2();

        // Читаем и печатаем реплику
        readPrintReplica(replicaSnapshotUlz);
        readPrintReplica(replicaSnapshotLic);

        // Копируем реплику для анализа
        File replicaSnapshotFileUlz = new File("../_test-data/ws_002_000000001-Ulz.zip");
        File replicaSnapshotFileLic = new File("../_test-data/ws_002_000000001-Lic.zip");
        FileUtils.copyFile(replicaSnapshotUlz.getFile(), replicaSnapshotFileUlz);
        FileUtils.copyFile(replicaSnapshotLic.getFile(), replicaSnapshotFileLic);


        // Ой! Кто-то удалил все нафиг!
        db2.execSql("delete from PawnChit where id <> 0");
        db2.execSql("delete from Lic where id <> 0");
        //
        UtData.outTable(db2.loadSql("select id, ChitNo, ChitDt from PawnChit order by id"));
        UtData.outTable(db2.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));


        // Не страшно, сейчас вернем!

        // Выполнение команды
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        ws2.useReplicaFile(replicaSnapshotFileLic, true);

        //
        UtData.outTable(db2.loadSql("select id, ChitNo, ChitDt from PawnChit order by id"));
        UtData.outTable(db2.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));
    }


    public IReplica createReplicaSnapshot_Ulz_ws2() throws Exception {
        return createReplicaTableSnapshot_ws2("ulz");
    }

    public IReplica createReplicaSnapshot_Lic_ws2() throws Exception {
        return createReplicaTableSnapshot_ws2("lic");
    }

    public IReplica createReplicaTableSnapshot_ws2(String tableName) throws Exception {

        // Правила публикации
        JSONObject cfgPublication = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_166.json"));
        String publicationName = (String) cfgPublication.get("out");
        JSONObject cfgPublicationOut = (JSONObject) cfgPublication.get(publicationName);
        //
        IPublicationStorage publicationStorage = new PublicationStorage();
        publicationStorage.loadRules(cfgPublicationOut, struct2);

        // Забираем установочную реплику
        long wsId = 2;
        UtRepl utRepl = new UtRepl(db2, struct2);
        IReplica replica = utRepl.createReplicaTableSnapshot(wsId, publicationStorage.getPublicationRule(tableName), 1, true);

        //
        return replica;
    }

    public static void readPrintReplica(IReplica replicaSnapshot) throws Exception {
        // Откроем Zip-файл реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File(replicaSnapshot.getFile().getAbsolutePath()));
        InputStream inputStream = JdxReplicaReaderXml.createInputStreamData(replica);

        // Читаем заголовки
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(inputStream);
        System.out.println("WsId = " + reader.getWsId());
        System.out.println("Age = " + reader.getAge());
        System.out.println("ReplicaType = " + reader.getReplicaType());

        // Читаем данные
        String tableName = reader.nextTable();
        while (tableName != null) {
            System.out.println("table [" + tableName + "]");

            //
            long count = 0;

            // Перебираем записи
            Map rec = reader.nextRec();
            StringBuffer sb = new StringBuffer();
            while (rec != null) {
                count++;
                //
                sb.setLength(0);
                Set<Map.Entry> es = rec.entrySet();
                for (Map.Entry x : es) {
                    if (sb.length() != 0) {
                        sb.append(", ");
                    }
                    String val = String.valueOf(x.getValue());
                    if (val.length() > 30) {
                        val = val.substring(0, 20) + "...";
                    }
                    sb.append(x.getKey() + ": " + val);
                }
                System.out.println("  " + sb);

                //
                rec = reader.nextRec();
            }

            //
            System.out.println(tableName + ".count: " + count);

            //
            tableName = reader.nextTable();
        }

        // Закроем читателя Zip-файла
        inputStream.close();
    }

}
