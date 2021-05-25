package jdtx.repl.main.api.filter;

import com.udojava.evalex.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.math.*;
import java.util.*;

public class RecordFilter implements IRecordFilter {

    IPublicationRule publicationRule;
    Expression filterExpression;

    static Log log = LogFactory.getLog("jdtx.RecordFilter");


    public RecordFilter(IPublicationRule publicationRule, String tableName, Map<String, String> filterParams) {
        this.publicationRule = publicationRule;

        String filterExpressionStr = publicationRule.getFilterExpression();
        if (filterExpressionStr == null) {
            filterExpression = new Expression("true");
        } else {
            filterExpression = new Expression(filterExpressionStr);
            //
            log.debug("Table: " + tableName);
            log.debug("FilterExpression: " + filterExpressionStr);
        }

        // tableName -> filterExpression.filterParams
        filterExpression.setVariable("PARAM_tableName", tableName);

        // filterParams -> filterExpression.filterParams
        mapToExpressionParams(filterParams, filterExpression);
    }


    @Override
    public boolean isMach(Map<String, Object> recValues) {
        int oprType = UtJdx.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
        filterExpression.setVariable("PARAM_oprType", new BigDecimal(oprType));

        // recValues -> filterExpression.filterParams
        recordToExpressionParams(recValues);

        //
        if (filterExpression.eval().equals(BigDecimal.ONE)) {

            return true;
        } else {
            return false;
        }
    }


    void mapToExpressionParams(Map<String, String> params, Expression expression) {
        // Map values -> expression.params
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            expression.setVariable("PARAM_" + fieldName, new BigDecimal(fieldValue));
        }
    }


    void recordToExpressionParams(Map<String, Object> recValues) {
        // recValues -> filterExpression.params
        for (IJdxField field : publicationRule.getFields()) {
            String fieldName = field.getName();
            Object fieldValue = recValues.get(fieldName);

            //
            if (fieldValue != null) {
                IJdxTable refTable = field.getRefTable();
                if (field.isPrimaryKey() || refTable != null) {
                    // Ссылка
                    JdxRef ref = JdxRef.parse((String) fieldValue);
                    filterExpression.setVariable("RECORD_OWNER_" + fieldName, new BigDecimal(ref.ws_id));
                    filterExpression.setVariable("RECORD_" + fieldName, new BigDecimal(ref.value));
                } else if (fieldValue instanceof Long || fieldValue instanceof Integer) {
                    // Целочисленное поле
                    filterExpression.setVariable("RECORD_" + fieldName, new BigDecimal(fieldValue.toString()));
                } else {
                    // Прочие поля
                    String fieldValueStr = fieldValue.toString();
                    if (fieldValueStr.length() > 0) {
                        // filterExpression.setVariable(fieldName, fieldValueStr);
                        // todo: com.udojava.evalex.Expression.isNumber ошибается для дат в строке.
                        // Пока это не важно - мы сейчас данные в фильтрах не используем
                    }
                }
            }
        }
    }


}
