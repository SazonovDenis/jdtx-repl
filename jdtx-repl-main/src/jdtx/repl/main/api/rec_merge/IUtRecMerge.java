package jdtx.repl.main.api.rec_merge;

import java.util.*;

/**
 * Искатель дубликатов записей и исполнитель их слияния
 */
public interface IUtRecMerge {

    /**
     * Заготовка для реализации без заранее прочитанной структуры,
     * но пока UtRecMerge требует struct в конструкторе
     *
     * @return Список таблиц в базе
     */
    Collection<String> loadTables();

    /**
     * Заготовка для реализации без заранее прочитанной структуры,
     * но пока UtRecMerge требует struct в конструкторе
     *
     * @return Список полей в таблице
     */
    Collection<String> loadTableFields(String tableName);

    /**
     * Найти дубликаты в таблице tableName с совпадениями в полях fieldNames
     *
     * @return Список дубликатов
     */
    Collection<RecDuplicate> findTableDuplicates(String tableName, Collection<String> fieldNames, boolean useNullValues) throws Exception;

    /**
     * Для найденных дубликатов duplicates предложить план слияния записей.
     *
     * @return План на слияние дубликатов
     */
    Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception;

    /**
     * Выполнить планы (задачи) на слияние
     *
     * @param plans        Список планов (задач) слияния.
     * @param resultWriter Для каждой таблицы (RecMergePlan.tableName) возвращает,
     *                     что пришлось сделать с каждой из зависимых от RecMergePlan.tableName таблиц,
     *                     чтобы выполнить каждый план из plans.
     */
    void execMergePlan(Collection<RecMergePlan> plans, RecMergeResultWriter resultWriter) throws Exception;

    /**
     * Откатить слияние
     *
     * @param resultReader результат выполнения задач на слияние (затронутые записи)
     */
    void revertExecMergePlan(RecMergeResultReader resultReader) throws Exception;

}
