Общее
---------------------------------


Переименовать sample.* в папке WEB-INF
sample._app.rt
sample._db-ini.rt
sample.log.properties


Переименовать cfg/sample.* в папке WEB-INF/cfg
cfg/sample.srv.json
cfg/sample.ws.json



Настройка почтового сервера
---------------------------------


Обязательно:

Создать структуру почтовых каталогов, опираясь на guid из cfg/sample.srv.json



Настройка БД сервера
---------------------------------


Обязательно:

В файле _app.rt включить сервер (bgtask name="server") и рабочую станцию (bgtask name="ws")

jc repl-info
jc repl-create -id:1

jc repl-add-ws -id:1 -name:"Sever"
jc repl-add-ws -id:2 -name:"ws filial 2"
jc repl-add-ws -id:3 -name:"ws filial 3"
...
и т.д.


Опционально:

jc repl-snapshot
jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



Настройка БД рабочей станции
---------------------------------



Обязательно:

В файле _app.rt отключить сервер (bgtask name="server"), стереть конфиг srv.json
В файле _app.rt включить рабочую станцию (bgtask name="ws")

jc repl-info
jc repl-create -id:XXX


Опционально:


jc repl-snapshot
jc repl-sync -dir:f:\jdtx-repl\ -mark:true



Запуск
---------------------------------

jc run



Разное
---------------------------------


-Xms256m -Xmx1024m