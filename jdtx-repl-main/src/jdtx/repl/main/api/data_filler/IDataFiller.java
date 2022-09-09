package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Генератор записей со случайными данными.
 * <p>
 * В качестве генератора можно передать либо экземпляр генератора {@link IFileldValueGenerator},
 * либо готовое значение.
 * <p>
 * Можно передать Collection генераторов (или готовых значений), при формировании записи будет случайно выбран один.
 */
public interface IDataFiller {

    /**
     * Для таблицы table создает запись, заполненную генераторами generators.
     */
    Map<String, Object> genRecord(IJdxTable table, Map<String, Object> generators);

    /**
     * Для каждого поля таблицы table создает заполнятель для генерации значений.
     *
     * @param generatorsDefault генераторы по умолчанию (для некоторых полей)
     */
    Map<String, Object> createGenerators(IJdxTable table, Map<String, Object> generatorsDefault) throws Exception;

}
