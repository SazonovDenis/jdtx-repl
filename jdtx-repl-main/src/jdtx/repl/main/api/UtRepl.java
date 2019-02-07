package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.io.*;

/**
 * Главное API репликатора
 */
public class UtRepl {

    Db db;
    IJdxDbStruct struct;

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
        UtAuditManager ut = new UtAuditManager(db, struct);
        return ut.markAuditAge();
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        UtAuditManager ut = new UtAuditManager(db, struct);
        return ut.getAuditAge();
    }


    /**
     * Собрать аудит и подготовить реплику по правилам публикации publication
     * от возраста ageFrom до возраста ageTo включительно.
     * todo: правила/страегия работы с файлами и вообще с получателем информации - в какой момент и кто выбирает файл
     */
    IReplica createReplica(IPublication publication, long ageFrom, long ageTo) throws Exception {
        File file = new File("temp/csv.xml");
        OutputStream ost = new FileOutputStream(file);
        JdxDataWriter wr = new JdxDataWriter(ost);

        //
        wr.setReplicaInfo(1, ageFrom, ageTo); //todo: правила/страегия работы с DbID

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
                    String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                    utrr.fillAuditData(stuctTableName, publicationFields, ageFrom, ageTo, wr);
                }
            }
        }


        //
        wr.close();

        //
        IReplica res = new Replica();
        res.setFile(file);

        //
        return res;
    }


    /**
     * Применить реплику на рабочей станции
     */
    void applyReplica(IReplica replica, IPublication publication) throws Exception {
        UtAuditApplyer utaa = new UtAuditApplyer(db, struct);
        utaa.applyAuditData(replica, publication, null);
    }


    /**
     * При включении новой БД в систему:
     * первая реплика для сервера готовится как реплика на вставку всех существующих записей в этой БД.
     */
    IReplica createReplicaFull(IPublication publication) {
        return null;
    }


}
