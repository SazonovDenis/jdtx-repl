package jdtx.repl.main.api.rec_merge;

import java.util.*;

/**
 * Исполнитель слияния дубликатов
 */
public interface IUtRecMerge {

    Collection<String> loadTables();

    Collection<String> loadTableFields(String tableName);

    /**
     * Найти дубликаты в таблице tableName с совпадениями в полях fieldNames
     *
     * @return Список дубликатов
     */
    Collection<RecDuplicate> findTableDuplicates(String tableName, String[] fieldNames) throws Exception;

    /**
     * Выполнить планы (задачи) на слияние
     *
     * @param plans Список планов (задач) слияния.
     * @return Для каждой таблицы (RecMergePlan.tableName) возвращает,
     * что пришлось сделать с каждой из зависимых от RecMergePlan.tableName таблиц, чтобы выполнить каждый план из plans.
     */
    MergeResultTableMap execMergePlan(Collection<RecMergePlan> plans, boolean doDelete) throws Exception;

    /**
     * Откатить слияние
     * @param taskResults результат выполнения задач на слияние (затронутые записи)
     */
    void revertExecMergePlan(MergeResultTableMap taskResults);

}
