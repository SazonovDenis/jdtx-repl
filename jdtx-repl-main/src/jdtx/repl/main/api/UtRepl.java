package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * Главное API репликатора
 * todo: это НЕ главное API - просто утилитный класс. Зачем он вообще нужен в таком виде - неясно
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

        // Запоминаем разрешенную структуру БД
        dbStructSave(struct);
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

    public DataStore getInfoSrv() throws Exception {
        DataStore st = db.loadSql(UtReplSql.sql_srv);
        return st;
    }

    /**
     * Искусственно увеличить возраст рабочей станции
     */
    public long incAuditAge() throws Exception {
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // Проверяем, что весь свой аудит мы уже выложили в очередь
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
        String fileNameTemplate = UtString.padLeft(String.valueOf(replica.getInfo().getWsId()), 3, '0') + "-" + UtString.padLeft(String.valueOf(replica.getInfo().getAge()), 9, '0');
        File outFile = File.createTempFile("~jdx-" + fileNameTemplate + "-", ".zip");
        outputStream = new FileOutputStream(outFile);

        // Zip-файл
        zipOutputStream = new ZipOutputStream(outputStream);

        // Zip-файл (заголовок)
        ZipEntry zipEntryHead = new ZipEntry("dat.info");
        zipOutputStream.putNextEntry(zipEntryHead);
        String json = replica.getInfo().toString();
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
    public IReplica createReplicaFromAudit(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaFromAudit, wsId: " + wsId + ", age: " + age);

        //
        UtAuditSelector utrr = new UtAuditSelector(db, struct, wsId);

        // Узнаем интервалы для аудита
        Map auditInfo = utrr.loadAutitIntervals(publication, age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc(UtDbComparer.calcDbStructCrc(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setDtFrom((DateTime) auditInfo.get("z_opr_dttm_from"));
        replica.getInfo().setDtTo((DateTime) auditInfo.get("z_opr_dttm_to"));
        replica.getInfo().setReplicaType(JdxReplicaType.IDE);

        // Стартуем запись реплики
        createOutput(replica);


        // Пишем заголовок
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);


        // Забираем аудит по порядку сортировки таблиц в struct
        JSONArray publicationData = publication.getData();
        for (IJdxTableStruct table : struct.getTables()) {
            String stuctTableName = table.getName();
            JSONObject publicationTable = null;
            boolean foundInPublication = false;
            for (int i = 0; i < publicationData.size(); i++) {
                publicationTable = (JSONObject) publicationData.get(i);
                String publicationTableName = (String) publicationTable.get("table");
                if (stuctTableName.compareToIgnoreCase(publicationTableName) == 0) {
                    foundInPublication = true;
                    break;
                }
            }

            //
            if (foundInPublication) {
                String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                //utrr.readAuditData_old(stuctTableName, publicationFields, age - 1, age, writerXml);

                // Интервал id в таблице аудита, который покрывает возраст age
                Map autitInfoTable = (Map) auditInfo.get(stuctTableName);
                if (autitInfoTable != null) {
                    long fromId = (long) autitInfoTable.get("z_id_from");
                    long toId = (long) autitInfoTable.get("z_id_to");

                    //
                    if (toId >= fromId) {
                        log.info("createReplicaFromAudit: " + stuctTableName + ", age: " + age + ", z_id: [" + fromId + ".." + toId + "], audit recs: " + (toId - fromId + 1));
                        //
                        utrr.readAuditData_ById(stuctTableName, publicationFields, fromId, toId, writerXml);
                    }
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
    public IReplica createReplicaSnapshot(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaSnapshot");

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc(UtDbComparer.calcDbStructCrc(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);

        // Открываем запись
        createOutput(replica);


        // Пишем
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);

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

    public IReplica createReplicaSetDbStruct() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_DB_STRUCT);
        //replica.setWsId(wsId);
        //replica.setAge(age);

        // Открываем запись
        createOutput(replica);

        // Файл с описанием текущей структуры БД
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        zipOutputStream.write(struct_rw.getBytes(struct));

        // Заканчиваем запись
        closeOutput();

        //
        return replica;
    }

    public IReplica createReplicaUnmute() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.UNMUTE);
        //replica.setWsId(wsId);
        //replica.setAge(age);

        // Открываем запись
        createOutput(replica);

        // Писать в файл нечего
        // ...

        // Заканчиваем запись
        closeOutput();

        //
        return replica;
    }

    public IReplica createReplicaMute() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.MUTE);
        //replica.setWsId(wsId);
        //replica.setAge(age);

        // Открываем запись
        createOutput(replica);

        // Писать в файл нечего
        // ...

        // Заканчиваем запись
        closeOutput();

        //
        return replica;
    }

    public static InputStream getReplicaInputStream(IReplica replica) throws IOException {
        return createInputStream(replica, ".xml");
    }

    public static InputStream createInputStream(IReplica replica, String dataFileMask) throws IOException {
        InputStream inputStream = null;
        ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(replica.getFile()));
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            String name = entry.getName();
            if (name.endsWith(dataFileMask)) {
                inputStream = zipInputStream;
                break;
            }
        }
        if (inputStream == null) {
            throw new XError("Not found [" + dataFileMask + "] in replica: " + replica.getFile());
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

    public void dbStructSaveFrom(InputStream stream) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStruct struct = struct_rw.read(stream);
        //
        dbStructSave(struct);
    }

    public void dbStructSaveFrom(File file) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        dbStructSave(struct);
    }

    public void dbStructSave(IJdxDbStruct struct) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        byte[] bytes = struct_rw.getBytes(struct);
        db.execSql("update Z_Z_state set db_struct = :db_struct where id = 1", UtCnv.toMap("db_struct", bytes));
    }

    public static String getVersion() {
        VersionInfo vi = new VersionInfo("jdtx.repl.main");
        return vi.getVersion();
    }

    public static boolean tableSkipRepl(IJdxTableStruct table) {
        return table.getPrimaryKey().size() == 0;
    }


}
