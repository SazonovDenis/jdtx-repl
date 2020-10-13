package jdtx.repl.main.api.rec_merge;

import java.util.*;

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
     * Выполнить задачи на слияние
     *
     * @param tasks Список задач на слияние
     * @return Для каждой RecMergeTask.tableName возвращает,
     * что пришлось сделать с каждой из зависимых от RecMergeTask.tableName таблиц, чтобы выполнить каждую task.
     */
    Map<String, MergeResultTable> execMergeTask(Collection<RecMergeTask> tasks, boolean doDelete) throws Exception;

}
