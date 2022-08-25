# Перенос записей primary key

Иногда возникает задача переместить primary key записи. 
Команда `rec-relocate` перемещает записи в таблице, автоматически исправляя ссылки на перемещаемые записи из зависимых таблиц.  


## Поведение команды

Проверяется, что запись с исходным primary key существует, а запись с конечным primary key – отсутствует. 
Если запись с конечным primary key уже существует, то она не будет затерта, а будет выдана ошибка. 

При переносе записи она не редактируется, а удаляется со старого primary key и добавляется по новому primary key.

Перенос записи и исправление ссылок либо происходит полностью успешно, либо не происходит совсем (выполняются в одной транзакции).

Если перенос записи выполнился успешно, то для нее будет сформирован файл (zip-архив с xml-файлом `dat.xml`), 
в котором перечислены все записи всех зависимых таблиц, которые пришлось изменить, чтобы переместить запись.
Имя файла, формируется из имени таблицы, начальной и конечного primary key, например: `relocate_WELL_1000_3000000000000.result.zip`.

<details>
<summary>Пример файла результата</summary>

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
</details>

Если при переносе записи возникла ошибка, то будет сформирован файл с текстом ошибки, например: `relocate_WELL_1000_3000000000000.error`

<details>
<summary>Пример файла с ошибкой</summary>

~~~
jandcode.utils.error.XError: Error relocateId: sour id WELL.1000 not found
~~~

</details>

Если одной командой перемещается несколько primary key, то ошибки одной преноса не останавливают выполнение переноса других.


## Параметры команды

Обязательно указать:

`table` - Имя таблицы\
`outDir` - Каталог с результатом. В этом каталоге на каждую перемещенную запись будет сформирован файл, 
в котором перечислены все записи всех зависимых таблиц, которые пришлось изменить, чтобы переместить запись.

Перемещаемые записи можно либо указать напрямую, либо перечислить в файле, либо задать диапазон перемещаемых primary key.


### Передача списка исходных и конечных primary key через параметры

`sour` - Исходные primary key (можно передать несколько через запятую);\
`dest` - Конечные primary key.

##### Пример

Перенос записи таблицы WELL с id 1000 на 3000000000000:

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -sour:1000 -dest:3000000000000
~~~

<details>
<summary>Результат выполнения команды</summary>

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

</details>


В каталоге `d:/temp` будет сформирован файл `relocate_WELL_1000_3000000000000.result.zip`.


##### Пример

Перенос двух записей таблицы WELL:

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -sour:1000,1001 -dest:3000000000000,3000000000001
~~~

Запись с id 1000 будет перенесена на 3000000000000, а id 1001 станет 3000000000001.


### Передача списка исходных и конечных primary key через файл

`file` - текстовый файл, в котором перечислены исходные и конечные primary key, каждая пара - в отдельной строке.

##### Пример

Перенос записей таблицы WELL по списку primary key в файле `d:/ids_well.csv`:

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -file:d:/ids_well.csv
~~~

<details>
<summary>Пример файла со списком исходных и конечных primary key</summary>

~~~
726;5000000000000
727;5000000000001
728;5000000000002
729;5000000000003
730;5000000000004
731;5000000000005
732;5000000000006
733;5000000000007
734;5000000000008
735;5000000000009
736;5000000000010
737;5000000000011
738;5000000000012
739;5000000000013
740;5000000000014
741;5000000000015
742;5000000000016
743;5000000000017
744;5000000000018
~~~

</details>


### Указание диапазона исходных primary key

`sourFrom` - Начало диапазона перемещаемых primary key;\
`sourTo` - Конец диапазона перемещаемых primary key;\
`dest` - Начальный primary key, куда будут перенесены записи.

##### Пример

Записи таблицы WELL в диапазоне id от 1000 до 1020 будут перенесены на id, начинающиеся с 5000000000000:

~~~
jc rec-relocate -outDir:d:/temp -table:WELL -sourFrom:1000 -sourTo:1020 -dest:5000000000000
~~~
