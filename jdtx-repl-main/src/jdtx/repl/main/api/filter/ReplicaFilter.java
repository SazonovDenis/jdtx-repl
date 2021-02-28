package jdtx.repl.main.api.filter;

import com.udojava.evalex.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.math.*;
import java.util.*;

public class ReplicaFilter implements IReplicaFilter {

    //
    Map<String, String> filterParams = new HashMap<>();

    //
    protected static Log log = LogFactory.getLog("jdtx.ReplicaFilter");


    @Override
    public Map<String, String> getFilterParams() {
        return filterParams;
    }

    @Override
    public IReplica convertReplicaForWs(IReplica replicaSrc, IPublicationStorage publicationRules) throws Exception {
        File replicaFile = replicaSrc.getFile();

        // Файл должен быть - иначе незачем делать
        if (replicaFile == null) {
            throw new XError("Invalid replicaSrc.file == null");
        }

        //
        ReplicaFile replicaRes = new ReplicaFile();

        //
        IReplicaInfo replicaInfo = replicaSrc.getInfo();

        //
        replicaRes.getInfo().assign(replicaInfo);

        //
        getFilterParams().put("wsAuthor", String.valueOf(replicaInfo.getWsId()));

        //
        if (replicaInfo.getReplicaType() == JdxReplicaType.SNAPSHOT || replicaInfo.getReplicaType() == JdxReplicaType.IDE) {

            //
            InputStream inputStream = null;
            try {
                // Распакуем XML-файл из Zip-архива
                inputStream = JdxReplicaReaderXml.createInputStreamData(replicaSrc);

                JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

                // Стартуем формирование файла реплики
                UtReplicaWriter replicaWriter = new UtReplicaWriter(replicaRes);
                replicaWriter.replicaFileStart();

                // Начинаем писать файл с данными
                JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

                // Копируем данные из реплики
                copyDataWithFilter(replicaReader, xmlWriter, publicationRules, getFilterParams());

                // Заканчиваем формирование файла реплики
                replicaWriter.replicaFileClose();

            } finally {
                // Закроем читателя Zip-файла
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        } else {
            // Тупое копирование файла (нечего фильтровать)
            File replicaResFile = UtReplicaWriter.createTempFileReplica(replicaRes);
            FileUtils.copyFile(replicaFile, replicaResFile);
            replicaRes.setFile(replicaResFile);
        }


        //
        return replicaRes;
    }

    // todo отдельный тест на copyDataWithFilter
    private void copyDataWithFilter(JdxReplicaReaderXml dataReader, JdxReplicaWriterXml dataWriter, IPublicationStorage publicationRule, Map<String, String> filterParams) throws Exception {
        String tableName = dataReader.nextTable();

        // Перебираем таблицы
        while (tableName != null) {

            IPublicationRule publicationRuleTable = publicationRule.getPublicationRule(tableName);

            if (publicationRuleTable == null) {
                // Пропускаем
                log.info("  skip, not found in publicationRule, table: " + tableName);
            } else {
                dataWriter.startTable(tableName);

                //
                String filterExpressionStr = publicationRuleTable.getFilterExpression();
                Expression filterExpression;
                if (filterExpressionStr == null) {
                    filterExpression = new Expression("true");
                } else {
                    filterExpression = new Expression(filterExpressionStr);
                }

                // tableName -> filterExpression.filterParams
                filterExpression.setVariable("PARAM_tableName", tableName);

                // filterParams -> filterExpression.filterParams
                mapToExpressionParams(filterParams, filterExpression);

                // Перебираем записи
                long count = 0;
                long countSkipped = 0;

                //
                Map<String, Object> recValues = dataReader.nextRec();
                //
                while (recValues != null) {
                    int oprType = UtJdx.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
                    filterExpression.setVariable("PARAM_oprType", new BigDecimal(oprType));

                    // recValues -> filterExpression.filterParams
                    recordToExpressionParams(recValues, publicationRuleTable, filterExpression);

                    //
                    if (filterExpression.eval().equals(BigDecimal.ONE)) {
                        //
                        dataWriter.appendRec();

                        // Тип операции
                        dataWriter.setOprType(oprType);

                        // Значения полей
                        for (IJdxField publicationField : publicationRuleTable.getFields()) {
                            String publicationFieldName = publicationField.getName();
                            dataWriter.setRecValue(publicationFieldName, recValues.get(publicationFieldName));
                        }
                    } else {
                        log.info("  Record was skipped: " + recValues);
                        countSkipped++;
                    }

                    //
                    recValues = dataReader.nextRec();

                    //
                    count++;
                    if (count % 200 == 0) {
                        log.info("  table: " + tableName + ", " + count);
                    }
                }

                //
                if (countSkipped == 0) {
                    log.info("  done: " + tableName + ", total: " + count + ", no skipped");
                } else {
                    log.info("  done: " + tableName + ", total: " + count + ", skipped: " + countSkipped);
                }

                //
                dataWriter.flush();
            }


            //
            tableName = dataReader.nextTable();
        }
    }

    private void mapToExpressionParams(Map<String, String> params, Expression expression) {
        // Map values -> expression.params
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            expression.setVariable("PARAM_" + fieldName, new BigDecimal(fieldValue));
        }
    }

    private void recordToExpressionParams(Map<String, Object> recValues, IPublicationRule publicationRule, Expression expression) {
        // recValues -> expression.params
        for (IJdxField field : publicationRule.getFields()) {
            String fieldName = field.getName();
            Object fieldValue = recValues.get(fieldName);

            //
            if (fieldValue != null) {
                IJdxTable refTable = field.getRefTable();
                if (field.isPrimaryKey() || refTable != null) {
                    // Ссылка
                    JdxRef ref = JdxRef.parse((String) fieldValue);
                    expression.setVariable("PARAM_wsAuthor_" + fieldName, new BigDecimal(ref.ws_id));
                    expression.setVariable("RECORD_" + fieldName, new BigDecimal(ref.id));
                } else if (fieldValue instanceof Long || fieldValue instanceof Integer) {
                    // Целочисленное поле
                    expression.setVariable("RECORD_" + fieldName, new BigDecimal(fieldValue.toString()));
                } else {
                    // Прочие поля
                    String fieldValueStr = fieldValue.toString();
                    if (fieldValueStr.length() > 0) {
                        // expression.setVariable(fieldName, fieldValueStr); todo: com.udojava.evalex.Expression.isNumber ошибается для дат в строке
                    }
                }
            }
        }
    }


}
