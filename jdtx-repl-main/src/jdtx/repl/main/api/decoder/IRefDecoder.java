package jdtx.repl.main.api.decoder;

/**
 * Перекодировщик ссылок.
 * При наличии разведения ID - не нужен,
 * в противном случае необходима реализация политики перекодировки.
 */
public interface IRefDecoder {

    /**
     * Превращает id от рабочей станции ws_id в свою id
     *
     * @param tableName имя таблицы
     * @param ws_id     код рабочей станции
     * @param id        id на этой рабочей станции
     * @return id на нашей станции
     */
    long get_id_own(String tableName, long ws_id, long id) throws Exception;

    /**
     * Превращает свою id в пару: код рабочей станции + id на этой станции
     *
     * @param tableName имя таблицы
     * @param own_id    id на нашей рабочей станции
     * @return объект JdxRef: код рабочей станции (владельца записи) + id на этой станции (владельца записи)
     */
    JdxRef get_ref(String tableName, long own_id) throws Exception;

    /**
     * Является ли эта id созданной на нашей рабочей станции
     */
    boolean is_own_id(String tableName, long own_id);

}
