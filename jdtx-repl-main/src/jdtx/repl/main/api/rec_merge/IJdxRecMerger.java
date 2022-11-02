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
     * @return список таблиц в базе
     */
    Collection<String> loadTables();

    /**
     * Заготовка для реализации без заранее прочитанной структуры,
     * но пока JdxRecMerge требует struct в конструкторе
     *
     * @return список полей в таблице
     */
    Collection<String> loadTableFields(String tableName);

    /**
     * Найти дубликаты в таблице tableName с совпадениями в полях fieldNames
     *
     * @return список дубликатов
     */
    Collection<RecDuplicate> findTableDuplicates(String tableName, String tableFields, boolean useNullValues) throws Exception;

    /**
     * Для найденных дубликатов duplicates предложить план слияния записей.
     *
     * @return план на слияние дубликатов
     */
    Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception;

    /**
     * Выполнить планы (задачи) на слияние
     *
     * @param plans      список планов (задач) слияния.
     * @param resultFile для каждой таблицы (RecMergePlan.tableName) возвращает,
     *                   что пришлось сделать с каждой из зависимых от RecMergePlan.tableName таблиц,
     *                   чтобы выполнить каждый план из plans.
     */
    void execMergePlan(Collection<RecMergePlan> plans, File resultFile) throws Exception;

    /**
     * Откатить выполненные задачи (слияние и т.д.)
     *
     * @param resultFile результат выполнения задач на слияние (затронутые записи)
     * @param tableNames какие таблицы восстанавливать. Передать null, если нужно восстановить все.
     */
    void revertExec(File resultFile, List<String> tableNames) throws Exception;

}
