package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

/**
 *
 */
public class UtRepl {

    Db db;

    UtRepl(Db db) {
        this.db = db;
    }


    /**
     * Создать репликационные структуры
     * - триггеры
     * - аудит
     * - таблица возрастов таблиц
     */
    void createReplication() throws Exception {
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        // создание всего
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.createAudit();
    }


    /**
     * Удалить репликационные структуры
     */
    void dropReplication() throws Exception {
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        // удаление всего
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.dropAudit();
    }

    /**
     * Задать возраст рабочей станции
     */
    void setAge(long age) {

    }

    /**
     * Узнать возраст рабочей станции
     */
    long getAge() {
        return 0;
    }


    /**
     * Собрать аудит и подготовить реплику по правилам публикации publication
     * от возраста auditFrom до возраста auditTo включительно.
     */
    IReplica createReplica(IPublication publication, long auditFrom, long auditTo) {
        return null;
    }


    /**
     * Применить реплику на рабочей станции
     */
    void applyReplica(IReplica replica) {
    }


    /**
     * При включении новой БД в систему:
     * первая реплика для сервера готовится как реплика на вставку всех существующих записей в этой БД.
     */
    IReplica createReplicaFull(IPublication publication) {
        return null;
    }


}
