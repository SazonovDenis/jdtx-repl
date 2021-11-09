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
    // todo: сделать так, чтобы можно было результат писать сразу В ФАЙЛ - чтобы память не расходовалась
    Collection<RecDuplicate> findTableDuplicates(String tableName, String[] fieldNames) throws Exception;

    /**
     * Для найденных дубликатов duplicates предложить план слияния записей.
     *
     * @return План на слияние дубликатов
     */
    // todo: сделать так, чтобы можно было результат писать сразу В ФАЙЛ - чтобы память не расходовалась
    Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception;

    /**
     * Выполнить планы (задачи) на слияние
     *
     * @param plans Список планов (задач) слияния.
     * @return Для каждой таблицы (RecMergePlan.tableName) возвращает,
     * что пришлось сделать с каждой из зависимых от RecMergePlan.tableName таблиц, чтобы выполнить каждый план из plans.
     */
    // todo: сделать так, чтобы можно было результат писать сразу В ФАЙЛ - чтобы память не расходовалась
    void execMergePlan(Collection<RecMergePlan> plans, RecMergeResultWriter resultWriter) throws Exception;

    /**
     * Откатить слияние
     *
     * @param taskResults результат выполнения задач на слияние (затронутые записи)
     */
    // todo: сделать так, чтобы можно было читать сразу ИЗ ФАЙЛА - чтобы память не расходовалась
    void revertExecMergePlan(RecMergeResultReader resultReader) throws Exception;

}
