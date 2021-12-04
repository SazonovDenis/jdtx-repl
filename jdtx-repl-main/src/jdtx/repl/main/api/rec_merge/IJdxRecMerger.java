package jdtx.repl.main.api.rec_merge;

import java.io.*;
import java.util.*;

/**
 * Искатель дубликатов записей и исполнитель их слияния
 */
public interface IJdxRecMerger {

    /**
     * Заготовка для реализации без заранее прочитанной структуры,
     * но пока JdxRecMerge требует struct в конструкторе
     *
     * @return Список таблиц в базе
     */
    Collection<String> loadTables();

    /**
     * Заготовка для реализации без заранее прочитанной структуры,
     * но пока JdxRecMerge требует struct в конструкторе
     *
     * @return Список полей в таблице
     */
    Collection<String> loadTableFields(String tableName);

    /**
     * Найти дубликаты в таблице tableName с совпадениями в полях fieldNames
     *
     * @return Список дубликатов
     */
    Collection<RecDuplicate> findTableDuplicates(String tableName, String tableFields, boolean useNullValues) throws Exception;

    /**
     * Для найденных дубликатов duplicates предложить план слияния записей.
     *
     * @return План на слияние дубликатов
     */
    Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception;

    /**
     * Выполнить планы (задачи) на слияние
     *
     * @param plans      Список планов (задач) слияния.
     * @param resultFile Для каждой таблицы (RecMergePlan.tableName) возвращает,
     *                   что пришлось сделать с каждой из зависимых от RecMergePlan.tableName таблиц,
     *                   чтобы выполнить каждый план из plans.
     */
    void execMergePlan(Collection<RecMergePlan> plans, File resultFile) throws Exception;

    /**
     * Откатить выполненные задачи (слияние и т.д.)
     *
     * @param resultFile Результат выполнения задач на слияние (затронутые записи)
     */
    void revertExec(File resultFile) throws Exception;

}
