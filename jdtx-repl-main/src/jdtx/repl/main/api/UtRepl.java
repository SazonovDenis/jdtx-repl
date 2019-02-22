package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;

/**
 * Главное API репликатора
 */
public class UtRepl {

    private Db db;
    private long dbId; // todo: правила/страегия работы с DbID
    private IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx");

    public UtRepl(Db db, long dbId) throws Exception {
        this.db = db;
        this.dbId = dbId;
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
    IReplica createReplicaFromAudit(IPublication publication, long age) throws Exception {
        log.info("createReplicaFromAudit");

        File file = new File("../_test-data/~tmp_csv.xml"); // todo: правила/страегия работы с файлами и вообще с получателем информации - в какой момент и кто выбирает файл
        OutputStream ost = new FileOutputStream(file);
        JdxReplicaWriterXml writerXml = new JdxReplicaWriterXml(ost);

        //
        writerXml.writeReplicaInfo(dbId, age);

        //
        UtAuditSelector utrr = new UtAuditSelector(db, struct);

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
        res.setDbId(dbId);
        res.setAge(age);
        res.setNo(age);
        res.setFile(file);

        //
        return res;
    }


    /**
     * При включении новой БД в систему:
     * Самая первая (установочная) реплика для сервера.
     * Готовится как реплика на вставку всех существующих записей в этой БД.
     */
    IReplica createReplicaFull(IPublication publication) throws Exception {
        File file = new File("../_test-data/csv_full.xml");
        OutputStream ost = new FileOutputStream(file);
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);

        //
        long age = 0; // Установочная реплика именно возраста 0
        wr.writeReplicaInfo(dbId, age);

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
        replica.setDbId(dbId);
        replica.setAge(age);
        replica.setFile(file);

        //
        return replica;
    }


    /**
     * Сервер: считывание очередей из рабочих  станций и формирование общей очереди
     */
    public void formCommonQue() {

    }

    /**
     * Сервер: распределение общей очереди по рабочим станциям
     */
    public void dispatchReplicas() {

    }


}
