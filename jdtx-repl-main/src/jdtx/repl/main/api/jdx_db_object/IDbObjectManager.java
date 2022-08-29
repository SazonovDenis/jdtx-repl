package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.struct.*;

/**
 * Создает служебные структуры в БД
 */
public interface IDbObjectManager {


    /**
     * Проверяем и обновляем версию БД (системных структур)
     */
    void checkVerDb() throws Exception;

    /**
     * Проверяем, что репликация инициализировалась
     */
    void checkReplicationInit() throws Exception;

    /**
     * Создает системные структуры и помечает рабочую станцию
     */
    void createReplBase(long wsId, String guidWs) throws Exception;

    /**
     * Удаляет системные структуры
     */
    void dropReplBase() throws Exception;


    /**
     * Создаем структуры для аудита:
     * для таблицы в БД создаем связанную с ней таблицу журнала изменений, триггеры и генератор.
     * <p>
     * Допускаем, что аудит уже был создан.
     * Так бывает, если по каким-то причинам следующий блок "выгрузка snapshot" будет прерван,
     * и в итоге реплика не будет помечена как использованная. Тогда ее применение начнется снова,
     * но структуры аудита снова создавать не надо.
     *
     * @param table Имя таблицы
     */
    void createAudit(IJdxTable table) throws Exception;

    /**
     * Удаляем структуры для аудита:
     * связанную с таблицей таблицу журнала изменений, триггеры и генератор.
     *
     * @param tableName Таблица
     */
    void dropAudit(String tableName) throws Exception;



}
