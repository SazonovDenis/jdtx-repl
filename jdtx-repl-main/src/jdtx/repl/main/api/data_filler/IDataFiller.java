package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Генератор записей со случайными данными.
 * <p>
 * В качестве генератора можно передать либо экземпляр генератора {@link IFieldValueGenerator},
 * либо готовое значение.
 * <p>
 * Можно передать Collection генераторов (или готовых значений), при формировании записи будет случайно выбран один.
 */
public interface IDataFiller {

    /**
     * Для таблицы table создает запись, заполненную генераторами tableGenerators.
     *
     * @param tableGenerators генераторы по умолчанию (для некоторых полей таблицы)
     */
    Map<String, Object> genRecord(IJdxTable table, Map<String, Object> tableGenerators) throws Exception;

    /**
     * Для каждого поля таблицы table создает заполнятель для генерации значений.
     *
     * @param tableGenerators генераторы по умолчанию (для некоторых полей таблицы)
     */
    Map<String, Object> createGenerators(IJdxTable table, Map<String, Object> tableGenerators) throws Exception;

    Map<String, Object> getGeneratorsCache();

    Map<String, Set<Long>> getRefValuesCache();

    String getFieldKey(IJdxField field);

    String getTableFieldKey(IJdxTable table, IJdxField field);

    String getRefKey(IJdxTable refTable);

    String getDatatypeKey(IJdxField field);

}
