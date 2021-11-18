package jdtx.repl.main.api.data_binder;

import java.util.*;

/**
 * Биндер для данных с навигацией по ним.
 */
public interface IJdxDataBinder {

    /**
     * @return значения
     */
    Map<String, Object> getValues();

    void next() throws Exception;

    boolean eof() throws Exception;

    void close() throws Exception;

}
