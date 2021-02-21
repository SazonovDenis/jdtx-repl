package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Публикация - правило подготовки реплик.
 * Публикация управляет подготовкой данных как наверх, так и вниз.
 * В общем виде правило - это просто запрос к аудиту
 * и left join к соответствующей таблице с данными, с наложением фильтров.
 */
public interface IPublicationRule {

    String getTableName();

    void setTableName(String tableName);

    Collection<IJdxField> getFields();

    String getAuthorWs();

    void setAuthorWs(String authorWs);

    String getFilterExpression();

    void setFilterExpression(String filterExpression);

}
