package jdtx.repl.main.api.filter;

import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;

import java.io.*;
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
    public IReplica convertReplicaForWs(IReplica replicaSrc, IPublicationRuleStorage publicationRules) throws Exception {
        File replicaFile = replicaSrc.getFile();

        // Файл должен быть - иначе незачем делать
        if (replicaFile == null) {
            throw new XError("Invalid replicaSrc.file == null");
        }

        log.info("convertReplicaForWs, replica.file: " + replicaSrc.getFile().getAbsolutePath());

        //
        ReplicaFile replicaRes = new ReplicaFile();

        //
        IReplicaInfo replicaInfo = replicaSrc.getInfo();

        //
        replicaRes.getInfo().assign(replicaInfo);

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
                JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

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
    private void copyDataWithFilter(JdxReplicaReaderXml dataReader, JdxReplicaWriterXml dataWriter, IPublicationRuleStorage publicationRules, Map<String, String> filterParams) throws Exception {
        String tableName = dataReader.nextTable();

        // Перебираем таблицы
        while (tableName != null) {
            IPublicationRule publicationRuleTable = publicationRules.getPublicationRule(tableName);

            if (publicationRuleTable == null) {
                // Пропускаем таблицу
                log.debug("  skip, not found in publicationRules, table: " + tableName);
            } else {
                dataWriter.startTable(tableName);

                //
                IRecordFilter recordFilter = new RecordFilter(publicationRuleTable, tableName, filterParams);

                // Перебираем записи
                long count = 0;
                long countSkipped = 0;

                //
                Map<String, String> recValuesStr = dataReader.nextRec();
                //
                while (recValuesStr != null) {
                    int oprType = UtData.intValueOf(recValuesStr.get(UtJdx.XML_FIELD_OPR_TYPE));

                    //
                    if (recordFilter.isMach(recValuesStr)) {
                        //
                        dataWriter.appendRec();

                        // Тип операции
                        dataWriter.writeOprType(oprType);

                        // Значения полей
                        UtXml.recToWriter(recValuesStr, UtJdx.fieldsToString(publicationRuleTable.getFields()), dataWriter);
                    } else {
                        countSkipped++;
                        //
                        log.debug("Record was skipped: " + recValuesStr);
                    }

                    //
                    recValuesStr = dataReader.nextRec();

                    //
                    count++;
                    if (count % 10000 == 0) {
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


}
