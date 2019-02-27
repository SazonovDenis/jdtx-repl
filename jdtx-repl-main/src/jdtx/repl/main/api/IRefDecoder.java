package jdtx.repl.main.api;

/**
 * Перекодировщик ссылок.
 * При наличии разведения ID - не нужен,
 * в противном случае необходима реализация политики перекодировки.
 */
public interface IRefDecoder {

    /**
     * @param db_id     id в сторонней БД
     * @param tableName Таблица
     * @return id в нашей БД
     */
    long get_id_own(long db_id, long ws_id, String tableName) throws Exception;

}
