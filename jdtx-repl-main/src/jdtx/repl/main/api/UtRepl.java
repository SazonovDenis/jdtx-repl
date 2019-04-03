package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.jdx_db_object.*;
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

    //
    private OutputStream outputStream = null;
    private ZipOutputStream zipOutputStream = null;
    private JdxReplicaWriterXml writerXml = null;


    public UtRepl(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
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
     * Искусственно увеличить возраст рабочей станции
     */
    public long incAuditAge() throws Exception {
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);

        // Проверяем, что весь свой аудит мы уже выложили в очередь
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long auditAgeDone = stateManager.getAuditAgeDone();
        long auditAgeActual = auditAgeManager.getAuditAge();
        if (auditAgeActual != auditAgeDone) {
            throw new XError("invalid auditAgeActual != auditAgeDone, auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);
        }

        //
        return auditAgeManager.incAuditAge();
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        return ut.getAuditAge();
    }

    void createOutput(IReplica replica) throws Exception {
        // Файл
        String fileNameTemplate = UtString.padLeft(String.valueOf(replica.getWsId()), 3, '0') + "-" + UtString.padLeft(String.valueOf(replica.getAge()), 9, '0');
        File outFile = File.createTempFile("~jdx-" + fileNameTemplate + "-", ".zip");
        outputStream = new FileOutputStream(outFile);

        // Zip-файл
        zipOutputStream = new ZipOutputStream(outputStream);

        // Zip-файл (заголовок)
        ZipEntry zipEntryHead = new ZipEntry("dat.info");
        zipOutputStream.putNextEntry(zipEntryHead);
        String json = "{\"wsId\": " + replica.getWsId() + ", \"age\": " + replica.getAge() + ", \"replicaType\": " + replica.getReplicaType() + "}";
        zipOutputStream.write(json.getBytes("utf-8"));
        zipOutputStream.closeEntry();

        // Zip-файл (данные)
        ZipEntry zipEntry = new ZipEntry("dat.xml");
        zipOutputStream.putNextEntry(zipEntry);

        // XML-файл
        writerXml = new JdxReplicaWriterXml(zipOutputStream);

        //
        replica.setFile(outFile);
    }

    void closeOutput() throws Exception {
        // Заканчиваем запись в XML-файл
        writerXml.close();

        // Заканчиваем запись в в zip-файл
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
        outputStream.close();
    }

    /**
     * Собрать аудит и подготовить реплику по правилам публикации publication
     * от для возраста age.
     */
    IReplica createReplicaFromAudit(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaFromAudit");

        //
        IReplica replica = new ReplicaFile();
        replica.setWsId(wsId);
        replica.setAge(age);
        replica.setReplicaType(JdxReplicaType.IDE);

        // Открываем запись
        createOutput(replica);


        // Пишем
        writerXml.startDocument();
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

        //
        writerXml.closeDocument();
        closeOutput();


        //
        return replica;
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
        IReplica replica = new ReplicaFile();
        replica.setWsId(wsId);
        replica.setAge(age);
        replica.setReplicaType(JdxReplicaType.SNAPSHOT);

        // Открываем запись
        createOutput(replica);


        // Пишем
        writerXml.startDocument();
        writerXml.writeReplicaInfo(wsId, age, JdxReplicaType.SNAPSHOT);

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


        // Заканчиваем запись
        writerXml.closeDocument();
        closeOutput();


        //
        return replica;
    }

    public IReplica createReplicaUnmute() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.setReplicaType(JdxReplicaType.UNMUTE);
        //replica.setWsId(wsId);
        //replica.setAge(age);

        // Открываем запись
        createOutput(replica);

        // Файл с описанием текущей структуры БД
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        zipOutputStream.write(struct_rw.write(struct));

        // Заканчиваем запись
        closeOutput();

        //
        return replica;
    }

    public IReplica createReplicaMute() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.setReplicaType(JdxReplicaType.MUTE);
        //replica.setWsId(wsId);
        //replica.setAge(age);

        // Открываем запись
        createOutput(replica);

        // Писать нечего

        // Заканчиваем запись
        closeOutput();

        //
        return replica;
    }

    public static InputStream getReplicaInputStream(IReplica replica) throws IOException {
        return createInputStream(replica, ".xml");
    }

    public static InputStream createInputStream(IReplica replica, String mask) throws IOException {
        InputStream inputStream = null;
        ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(replica.getFile()));
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            String name = entry.getName();
            if (name.endsWith(mask)) {
                inputStream = zipInputStream;
                break;
            }
        }
        if (inputStream == null) {
            throw new XError("Not found [" + mask + "] in replica: " + replica.getFile());
        }

        return inputStream;
    }

    public IJdxDbStruct dbStructLoad() throws Exception {
        String sql = "select db_struct from Z_Z_state where id = 1";
        DataStore st = db.loadSql(sql);
        byte[] db_struct = (byte[]) st.getCurRec().getValue("db_struct");
        //
        if (db_struct.length == 0) {
            return null;
        }
        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        return struct_rw.read(db_struct);
    }

    public void dbStructSave(File file) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        byte[] db_struct = struct_rw.write(struct);
        db.execSql("update Z_Z_state set db_struct = :db_struct where id = 1", UtCnv.toMap("db_struct", db_struct));
    }

    public void dbStructSave(IJdxDbStruct struct) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        byte[] db_struct = struct_rw.write(struct);
        db.execSql("update Z_Z_state set db_struct = :db_struct where id = 1", UtCnv.toMap("db_struct", db_struct));
    }


}
