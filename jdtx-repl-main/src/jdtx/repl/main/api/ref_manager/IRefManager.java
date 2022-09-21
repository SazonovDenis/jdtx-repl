package jdtx.repl.main.api.ref_manager;

import jdtx.repl.main.api.struct.*;

/**
 * Превращает локальные ссылки в глобальные JdxRef и наоборот.
 * Выражает особенности разведения PK между рабочими станциями в приложении.
 * <p>
 * Имеет несколько реализаций.
 * При некоторых способах разведения id  значения id должны быть меньше определенного числа,
 * при других - находится в определеном диапазоне.
 * <p>
 * - с пререкодировкой ссылок (на всех станциях pk собственнеых записей начинаются с 1,
 * чужие записи принимаются с перекодировкой)
 * <p>
 * - на основе диапазонов (???)
 * <p>
 * - пустая реализация (при наличии внешнего разведения pk).
 */
public interface IRefManager {

    /**
     * Превращает глобальную ссылку (id от рабочей станции ws_id) в свою локальную id_local
     *
     * @param tableName имя таблицы
     * @param ref       глобальная ссылка
     * @return локальная id на нашей станции
     */
    long get_id_local(String tableName, JdxRef ref) throws Exception;

    /**
     * Превращает свою (локальную) id_local в глобальную ссылку (пару: код рабочей станции (владельца записи) + id на этой станции)
     *
     * @param tableName имя таблицы
     * @param id_local  id на нашей рабочей станции
     * @return глобальная ссылка (пара: код рабочей станции (владельца записи) + id на этой станции)
     */
    JdxRef get_ref(String tableName, long id_local) throws Exception;

    /**
     * Для таблицы table возврящает последний занятый собственный id для станции (максмальное значение PK).
     */
    long get_max_own_id(IJdxTable table) throws Exception;

    /**
     * Проверяет, что в таблице table нет чужих id (от других станций).
     */
    boolean isPresent_not_own_id(IJdxTable table) throws Exception;

    /**
     * Получить уже инициализированный экземпляр
     */
    IRefManager getInstance() throws Exception;

}
