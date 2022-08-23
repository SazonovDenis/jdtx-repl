### Перенос записей primary key

Иногда возникает задача переместить primary key записи. Это делает команда `rec-relocate`

Параметры команды:

~~~
table - Имя таблицы
sour - Исходные primary key
dest - Конечные primary key
outDir - Каталог с результатом, например -dir:"d:/temp"
~~~

Пример:

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -sour:1000 -dest:3000000000000
~~~

Команда переносит запись с id 1000 на 3000000000000 и заменяет все ссылки на нее у всех таблиц:

~~~
Records sour:
+----+----------+--------+--------+----------------+----------------+-------------+
| ID |    DT    |  UWI   |ALTITUDE|     SURFX      |     SURFY      |ALTITUDEROTOR|
+----+----------+--------+--------+----------------+----------------+-------------+
|1000|1990-11-21|UZN_7122|   232.6|52.8840830869131|43.4032272700056|       <NULL>|
+----+----------+--------+--------+----------------+----------------+-------------+
records: 1
2022.08.23 08:58:52,340 INFO         UtRecMerger    Dependences for: WELL, records: 1
2022.08.23 08:58:54,444 INFO         UtRecMerger    Dependences for: WELL done
Records dest:
+-------------+----------+--------+--------+----------------+----------------+-------------+
|     ID      |    DT    |  UWI   |ALTITUDE|     SURFX      |     SURFY      |ALTITUDEROTOR|
+-------------+----------+--------+--------+----------------+----------------+-------------+
|3000000000000|1990-11-21|UZN_7122|   232.6|52.8840830869131|43.4032272700056|       <NULL>|
+-------------+----------+--------+--------+----------------+----------------+-------------+
records: 1
~~~

В каталоге `d:/temp` будет сформирован файл `relocate_WELL_1000_3000000000000.result.zip`.
Это zip-архив с xml-файлом `dat.xml`, в котором перечислены все записи всех зависимых таблиц, 
которые пришлось изменить, чтобы переместить запись в таблице `WELL` из id `1000` на `3000000000000`:

~~~xml

<?xml version="1.0" ?>
<root>
    <table name="WELL" operation="DEL">
        <rec DT="1990-11-21" UWI="UZN_7122" SURFY="43.4032272700056" ID="1000" ALTITUDE="232.6" SURFX="52.8840830869131"></rec>
    </table>

    <table name="ABSORB_DATA" operation="UPD" info="ref: ABSORB_DATA.WELL_ID--WELL"></table>
    
    <table name="ARR_DAILY" operation="UPD" info="ref: ARR_DAILY.WELL_ID--WELL"></table>

    <table name="ASMA_T_REG" operation="UPD" info="ref: ASMA_T_REG.WELL_ID--WELL">
        <rec DT="2021-03-06" PRESSURE="3.1" GAS_DEBIT="3180.8" GAS_FACTOR="157.47" HUMIDITY="61.2" WELL_ID="1000"
             TEMPERATURE="24.0" HOUR_LENGTH="24" OIL_DEBIT="20.2" WATER_DEBIT="38.7" ID="2000000043365"
             LIQUID_DEBIT="59.1"></rec>
    </table>

    <table name="BSW" operation="UPD" info="ref: BSW.WELL_ID--WELL">
        <rec WELL_ID="1000" DEND="2007-07-11" ID="89182" BSW_VAL="60.0" SOURCE_TYPE_ID="1" DBEG="2007-07-05"></rec>
        <rec WELL_ID="1000" DEND="2007-07-18" ID="89183" BSW_VAL="90.0" SOURCE_TYPE_ID="1" DBEG="2007-07-11"></rec>
        <rec WELL_ID="1000" DEND="2007-08-03" ID="89184" BSW_VAL="52.0" SOURCE_TYPE_ID="1" DBEG="2007-07-18"></rec>

        ...

        <rec WELL_ID="1000" DEND="3333-12-31" ID="2000024044665" BSW_VAL="93.0" SOURCE_TYPE_ID="1" DBEG="2022-08-06"></rec>
    </table>
    
    <table name="BSW_LAB" operation="UPD" info="ref: BSW_LAB.WELL_ID--WELL">
        <rec DT="2016-01-03" WATTER_CONSTRAINED_5_ML="0.0" WELL_ID="1000" FREE_WATTER_ML="0.0"
             WATTER_CONSTRAINED_PERC="0.0" WATTER_CONSTRAINED_ML="0.0" EMULSION_ML="0.0" ID="2000000015041"
             BSW_LAB_VAL="0.0" BSW_VOLUME_ML="0.0" BSW_VOLUME_PERC="78.0" PROBA_ML="0.0"></rec>
        <rec DT="2016-01-10" WATTER_CONSTRAINED_5_ML="0.0" WELL_ID="1000" FREE_WATTER_ML="0.0"
             WATTER_CONSTRAINED_PERC="0.0" WATTER_CONSTRAINED_ML="0.0" EMULSION_ML="0.0" ID="2000000044771"
             BSW_LAB_VAL="0.0" BSW_VOLUME_ML="0.0" BSW_VOLUME_PERC="82.0" PROBA_ML="0.0"></rec>
        <rec DT="2016-01-17" WATTER_CONSTRAINED_5_ML="0.0" WELL_ID="1000" FREE_WATTER_ML="340.0"
             WATTER_CONSTRAINED_PERC="0.0" WATTER_CONSTRAINED_ML="0.0" EMULSION_ML="20.0" ID="2000000075761"
             BSW_LAB_VAL="94.44444444" BSW_VOLUME_ML="340.0" BSW_VOLUME_PERC="94.44444444" PROBA_ML="360.0"></rec>

        ...

    </table>
</root>
~~~


Команда допускает перенос сразу нескольких primary key: 

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -sour:1000,1001 -dest:3000000000000,3000000000001
~~~
