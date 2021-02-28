package jdtx.repl.main.api.data_binder;

/**
 * Биндер для данных с навигацией по ним.
 */
public interface IJdxDataBinder {

    /**
     * @param name имя
     * @return значение
     */
    Object getValue(String name);

    void next() throws Exception;

    boolean eof() throws Exception;

    void close() throws Exception;

}
