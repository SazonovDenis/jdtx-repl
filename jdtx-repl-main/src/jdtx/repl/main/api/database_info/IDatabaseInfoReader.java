package jdtx.repl.main.api.database_info;

/**
 * "Логическая" версия базы данных.
 * Репликатором никак не используется, просто для удобства мониторинга.
 */
public interface IDatabaseInfoReader {

    /**
     * Читает "логическую" версию базы данных.
     */
    String readDatabaseVersion() throws Exception;

}
