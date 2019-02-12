package jdtx.repl.main.api;

/**
 * Перекодировщик ссылок.
 * При наличии разведения ID полностью вырожден,
 * в противном случае необходима реализация политики перекодировки.
 */
public interface IRefDecoder {

    long getOrCreate_id_own(long db_id, String tableName) throws Exception;

    long get_id_own(long db_id, String tableName);

}
