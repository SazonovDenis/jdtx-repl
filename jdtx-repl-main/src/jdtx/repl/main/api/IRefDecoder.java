package jdtx.repl.main.api;

/**
 * Перекодировщик ссылок.
 * При наличии разведения ID - не нужен,
 * в противном случае необходима реализация политики перекодировки.
 */
public interface IRefDecoder {

    /**
     * Превращает db_id от рабочей станции ws_id в свою id
     *
     * @param db_id     id в другой рабочей станции
     * @param ws_id     код рабочей станции
     * @param tableName имя таблицы
     * @return id в нашей БД
     */
    long get_id_own(long db_id, long ws_id, String tableName) throws Exception;

    /**
     * Превращает свою id в пару: рабочей станция + id на этой станции
     *
     * @param own_id    id на нашей рабочей станции
     * @param tableName имя таблицы
     * @return объект JdxRef: код рабочей станции + id на этой станции
     */
    JdxRef get_ref(long own_id, String tableName) throws Exception;

}
