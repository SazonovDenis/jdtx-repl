package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

/**
 * Генератор значений (обычно случайных по некоторому шаблону)
 */
public interface IFileldValueGenerator {

    Object genValue(IJdxField field);

}
