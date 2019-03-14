package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;

/**
 * Главное API репликатора
 * todo: НЕ главное API - просто утилитный класс. Зачем он вообще нужен в таком виде - неясно
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
    public void createReplication() throws Exception {
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
    public void dropReplication() throws Exception {
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

        //
        File file = File.createTempFile("~jdx-" + UtString.padLeft(String.valueOf(wsId), 3, '0') + "-" + UtString.padLeft(String.valueOf(age), 9, '0') + "-", ".xml");
        OutputStream outputStream = new FileOutputStream(file);
        JdxReplicaWriterXml writer = new JdxReplicaWriterXml(outputStream);

        //
        writer.writeReplicaInfo(wsId, age, JdxReplicaType.IDE);

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
                    String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                    utrr.readAuditData(stuctTableName, publicationFields, age - 1, age, writer);
                }
            }
        }


        //
        writer.close();
        outputStream.close();

        //
        IReplica res = new ReplicaFile();
        res.setWsId(wsId);
        res.setAge(age);
        res.setReplicaType(JdxReplicaType.IDE);
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
    IReplica createReplicaSnapshot(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaSnapshot");

        //
        File file = File.createTempFile("~jdx-" + UtString.padLeft(String.valueOf(wsId), 3, '0') + "-" + UtString.padLeft(String.valueOf(age), 9, '0') + "-full", ".xml");
        OutputStream outputStream = new FileOutputStream(file);
        JdxReplicaWriterXml writer = new JdxReplicaWriterXml(outputStream);

        //
        writer.writeReplicaInfo(wsId, age, JdxReplicaType.EXPORT);

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
                    utrr.readAllRecords(stuctTableName, publicationFields, writer);
                }
            }
        }

        //
        writer.close();
        outputStream.close();

        //
        IReplica replica = new ReplicaFile();
        replica.setWsId(wsId);
        replica.setAge(age);
        replica.setReplicaType(JdxReplicaType.EXPORT);
        replica.setFile(file);

        //
        return replica;
    }


}
