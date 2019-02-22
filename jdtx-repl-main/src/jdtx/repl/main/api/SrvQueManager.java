package jdtx.repl.main.api;

/**
 * Серверный обработчик очередей.
 * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
 * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
 */
public class SrvQueManager {

    IJdxQuePersonal localOutQue;

}
