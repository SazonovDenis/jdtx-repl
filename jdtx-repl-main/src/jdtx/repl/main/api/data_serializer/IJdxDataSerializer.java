package jdtx.repl.main.api.data_serializer;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public interface IJdxDataSerializer {

    /**
     * Приготовиться к обработке значений указанной таблицы.
     * Указываем перечень полей, чтобы удобно было обработать запись целиком.
     *
     * @param tableFieldNamesStr Поля, которые нужно обрабатывать
     */
    // todo: почему table как объект, а поля - как строка?
    void setTable(IJdxTable table, String tableFieldNamesStr);

    /**
     * Сериализация значения в строку.
     *
     * @param fieldValue Типизированное значение или ссылка
     * @return Строковое значение
     */
    String prepareValueStr(Object fieldValue, IJdxField field) throws Exception;

    /**
     * Десериализация значения из строки.
     *
     * @param fieldValueStr строковое значение
     * @return Типизированное значение, разрешение глобальных сылок до локальных
     */
    Object prepareValue(String fieldValueStr, IJdxField field) throws Exception;

    /**
     * Сериализация значений записи.
     */
    Map<String, String> prepareValuesStr(Map<String, Object> values) throws Exception;

    /**
     * Десериализация значений записи.
     */
    Map<String, Object> prepareValues(Map<String, String> valuesStr) throws Exception;

    /**
     * Получить уже инициализированный экземпляр
     */
    IJdxDataSerializer getInstance() throws Exception;

}
