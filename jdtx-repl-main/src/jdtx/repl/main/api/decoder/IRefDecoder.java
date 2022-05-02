package jdtx.repl.main.api.decoder;

/**
 * Перекодировщик ссылок.
 * При наличии разведения ID - не нужен,
 * в противном случае необходима реализация политики перекодировки.
 */
public interface IRefDecoder {

    /**
     * Превращает глобальную ссылку (id от рабочей станции ws_id) в свою локальную id
     *
     * @param tableName имя таблицы
     * @param ref       глобальная ссылка
     * @return локальная id на нашей станции
     */
    long get_id_local(String tableName, JdxRef ref) throws Exception;

    /**
     * Превращает свою (локальную) id в глобальную ссылку (пару: код рабочей станции (владельца записи) + id на этой станции)
     *
     * @param tableName имя таблицы
     * @param id_local  id на нашей рабочей станции
     * @return глобальная ссылка (пара: код рабочей станции (владельца записи) + id на этой станции)
     */
    JdxRef get_ref(String tableName, long id_local) throws Exception;

    /**
     * Является ли эта id созданной на нашей рабочей станции
     */
    boolean is_own_id(String tableName, long id);

}
