package jdtx.repl.main.api.filter;

import com.udojava.evalex.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
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
    public boolean isMach(Map<String, String> recValues) {
        int oprType = UtData.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
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


    void recordToExpressionParams(Map<String, String> recValues) {
        // recValues -> filterExpression.params
        for (IJdxField field : publicationRule.getFields()) {
            String fieldName = field.getName();
            String fieldValue = recValues.get(fieldName);

            //
            if (fieldValue != null) {
                IJdxTable refTable = field.getRefTable();
                if (field.isPrimaryKey() || refTable != null) {
                    // Ссылка
                    JdxRef fieldValueRef = JdxRef.parse(fieldValue);
                    filterExpression.setVariable("RECORD_OWNER_" + fieldName, new BigDecimal(fieldValueRef.ws_id));
                    filterExpression.setVariable("RECORD_" + fieldName, new BigDecimal(fieldValueRef.value));
                } else if (field.getJdxDatatype() == JdxDataType.INTEGER) {
                    // Целочисленное поле
                    filterExpression.setVariable("RECORD_" + fieldName, new BigDecimal(fieldValue));
                } else {
                    // Прочие поля
                    if (fieldValue.length() > 0) {
                        // filterExpression.setVariable(fieldName, fieldValueStr);
                        // todo: com.udojava.evalex.Expression.isNumber ошибается для дат в строке.
                        // Пока это не важно - мы сейчас даты в фильтрах не используем
                    }
                }
            }
        }
    }


}
