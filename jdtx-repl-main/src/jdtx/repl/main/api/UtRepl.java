package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;
import java.util.zip.*;

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
     * Инициализация рабочей станции:
     * <p>
     * Создать репликационные структуры
     * - триггеры и таблицы аудита
     * - таблица возрастов таблиц
     * - таблицы перекодировок
     * <p>
     * Поставить метку wsId
     */
    public void createReplication(long wsId, String guid) throws Exception {
        // создание всего аудита
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.createRepl(wsId, guid);

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

        // Файл
        String fileName = UtString.padLeft(String.valueOf(wsId), 3, '0') + "-" + UtString.padLeft(String.valueOf(age), 9, '0');
        File file = File.createTempFile("~jdx-" + fileName + "-", ".zip");
        OutputStream outputStream = new FileOutputStream(file);

        // Zip-файл
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);

        // Zip-файл (заголовок)
        ZipEntry zipEntryHead = new ZipEntry("dat.json");
        zipOutputStream.putNextEntry(zipEntryHead);
        String json = "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + JdxReplicaType.IDE + "}";
        zipOutputStream.write(json.getBytes("utf-8"));
        zipOutputStream.closeEntry();

        // Zip-файл (данные)
        ZipEntry zipEntry = new ZipEntry("dat.dat");
        zipOutputStream.putNextEntry(zipEntry);

        // XML-файл
        JdxReplicaWriterXml writerXml = new JdxReplicaWriterXml(zipOutputStream);

        //
        writerXml.writeReplicaInfo(wsId, age, JdxReplicaType.IDE);

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
                    utrr.readAuditData(stuctTableName, publicationFields, age - 1, age, writerXml);
                }
            }
        }


        // Заканчиваем запись в XML-файл
        writerXml.close();

        // Заканчиваем запись в в zip-файл
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
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

        // Файл
        String fileName = UtString.padLeft(String.valueOf(wsId), 3, '0') + "-" + UtString.padLeft(String.valueOf(age), 9, '0') + "-full";
        File file = File.createTempFile("~jdx-" + fileName + "-", ".zip");
        OutputStream outputStream = new FileOutputStream(file);

        // Zip-файл
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);

        // Zip-файл (заголовок)
        ZipEntry zipEntryHead = new ZipEntry("dat.info");
        zipOutputStream.putNextEntry(zipEntryHead);
        String info = "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + JdxReplicaType.EXPORT + "}";
        zipOutputStream.write(info.getBytes("utf-8"));
        zipOutputStream.closeEntry();

        // Zip-файл (данные)
        ZipEntry zipEntry = new ZipEntry("dat.dat");
        zipOutputStream.putNextEntry(zipEntry);

        // XML-файл
        JdxReplicaWriterXml writerXml = new JdxReplicaWriterXml(zipOutputStream);

        //
        writerXml.writeReplicaInfo(wsId, age, JdxReplicaType.EXPORT);

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
                    utrr.readAllRecords(stuctTableName, publicationFields, writerXml);
                }
            }
        }

        // Заканчиваем запись в XML-файл
        writerXml.close();

        // Заканчиваем запись в в zip-файл (данные)
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
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
