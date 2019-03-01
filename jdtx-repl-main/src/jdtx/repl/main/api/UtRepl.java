package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Главное API репликатора
 */
public class UtRepl {

    private Db db;
    private IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx");

    public UtRepl(Db db) throws Exception {
        this.db = db;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
    }


    /**
     * Создать репликационные структуры
     * - триггеры и таблицы аудита
     * - таблица возрастов таблиц
     * - таблицы перекодировок
     */
    void createReplication() throws Exception {
        // создание всего аудита
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.createAudit();

        // создаем необходимые для перекодировки таблицы
        UtDbObjectDecodeManager decodeManager = new UtDbObjectDecodeManager(db);
        decodeManager.createRefDecodeObject();
    }


    /**
     * Удалить репликационные структуры
     */
    void dropReplication() throws Exception {
        // удаление всего аудита
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.dropAudit();

        // удаляем необходимые для перекодировки таблицы
        UtDbObjectDecodeManager decodeManager = new UtDbObjectDecodeManager(db);
        decodeManager.dropRefDecodeObject();

    }

    /**
     * Зафиксировать возраст рабочей станции
     */
    public long markAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        return ut.markAuditAge();
    }


    /**
     * Зафиксировать возраст рабочей станции
     */
    public long incAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        return ut.incAuditAge();
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        return ut.getAuditAge();
    }


    /**
     * Собрать аудит и подготовить реплику по правилам публикации publication
     * от для возраста age.
     */
    IReplica createReplicaFromAudit(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaFromAudit");

        File file = new File("../_test-data/~tmp-" + wsId + "-" + age + "-csv.xml"); // todo: правила/страегия работы с файлами и вообще с получателем информации - в какой момент и кто выбирает файл
        OutputStream ost = new FileOutputStream(file);
        JdxReplicaWriterXml writerXml = new JdxReplicaWriterXml(ost);

        //
        writerXml.writeReplicaInfo(wsId, age);

        //
        UtAuditSelector utrr = new UtAuditSelector(db, struct, wsId);

        // Забираем аудит по порядку сортировки таблиц в struct
        JSONArray publicationData = publication.getData();
        for (IJdxTableStruct table : struct.getTables()) {
            String stuctTableName = table.getName();
            for (int i = 0; i < publicationData.size(); i++) {
                JSONObject publicationTable = (JSONObject) publicationData.get(i);
                String publicationTableName = (String) publicationTable.get("table");
                if (stuctTableName.compareToIgnoreCase(publicationTableName) == 0) {
                    log.info("readAuditData: " + stuctTableName);

                    //
                    String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                    utrr.readAuditData(stuctTableName, publicationFields, age - 1, age, writerXml);
                }
            }
        }


        //
        writerXml.close();

        //
        IReplica res = new ReplicaFile();
        res.setWsId(wsId);
        res.setAge(age);
        res.setFile(file);

        //
        return res;
    }


    /**
     * Реплика на вставку всех существующих записей в этой БД.
     * <p>
     * Используется при включении новой БД в систему:
     * самая первая (установочная) реплика для сервера.
     */
    IReplica createReplicaFull(long wsId, IPublication publication, long age) throws Exception {
        File file = new File("../_test-data/~tmp-" + wsId + "-" + age + "-full-csv.xml");  //todo: политика размещения!!
        OutputStream ost = new FileOutputStream(file);
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);

        //
        wr.writeReplicaInfo(wsId, age);

        //
        UtDataSelector utrr = new UtDataSelector(db, struct);

        // Забираем все данные из таблиц по порядку сортировки таблиц в struct
        JSONArray publicationData = publication.getData();
        for (IJdxTableStruct table : struct.getTables()) {
            String stuctTableName = table.getName();

            for (int i = 0; i < publicationData.size(); i++) {
                JSONObject publicationTable = (JSONObject) publicationData.get(i);
                String publicationTableName = (String) publicationTable.get("table");
                if (stuctTableName.compareToIgnoreCase(publicationTableName) == 0) {
                    String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                    utrr.readFullData(stuctTableName, publicationFields, wr);
                }
            }
        }

        //
        wr.close();

        //
        IReplica replica = new ReplicaFile();
        replica.setWsId(wsId);
        replica.setAge(age);
        replica.setFile(file);

        //
        return replica;
    }


    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    public void srvFillCommonQue(Map<Long, IJdxMailer> mailerList, IJdxQueCommon commonQue) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IJdxMailer mailer = (IJdxMailer) en.getValue();

            //
            long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
            long queMaxAge = mailer.getSrvReceive();

            //
            log.info("srvFillCommonQue, wsId: " + wsId + ", queDoneAge: " + queDoneAge + ", queMaxAge: " + queMaxAge);

            //
            for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                log.info("srvFillCommonQue, wsId: " + wsId + ",  age: " + age);

                //
                IReplica replica = mailer.receive(age);

                //
                db.startTran();
                try {
                    commonQue.put(replica);
                    //
                    stateManager.setWsQueInAgeDone(wsId, age);
                    //
                    db.commit();
                } catch (Exception e) {
                    db.rollback();
                    throw e;
                }
            }
        }
    }

    /**
     * Сервер: распределение общей очереди по рабочим станциям
     */
    public void srvDispatchReplicas(IJdxQueCommon commonQue, Map<Long, IJdxMailer> mailerList) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IJdxMailer mailer = (IJdxMailer) en.getValue();

            //
            long commonQueDoneNo = stateManager.getCommonQueNoDone(wsId);
            long commonQueMaxNo = commonQue.getMaxNo();

            //
            log.info("srvDispatchReplicas, wsId: " + wsId + ", commonQueDoneNo: " + commonQueDoneNo + ", commonQueMaxNo: " + commonQueMaxNo);

            //
            for (long no = commonQueDoneNo + 1; no <= commonQueMaxNo; no++) {
                log.info("srvDispatchReplicas, wsId: " + wsId + ", no: " + no);

                IReplica replica = commonQue.getByNo(no);

                mailer.send(replica, no);

                stateManager.setCommonQueNoDone(wsId, no);
            }
        }
    }


}
