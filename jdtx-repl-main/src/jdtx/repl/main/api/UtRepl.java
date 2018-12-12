package jdtx.repl.main.api;

import jandcode.dbm.db.*;

/**
 *
 */
public class UtRepl {

    DbUtils ut;

    UtRepl(DbUtils ut) {
        this.ut = ut;
    }


    /**
     * Создать репликационные структуры
     * - триггеры
     * - аудит
     * - таблица возрастов таблиц
     */
    void createReplication() {

    }


    /**
     * Удалить репликационные структуры
     */
    void dropReplication() {

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
