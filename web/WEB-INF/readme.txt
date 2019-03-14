Общее
---------------------------------


Переименовать sample.* в папке WEB-INF
sample._app.rt
sample._db-ini.rt
sample.log.properties


Переименовать cfg/sample.* в папке WEB-INF/cfg
cfg/sample.srv.json
cfg/sample.ws.json



Настройка сервера
---------------------------------


Обязательно:

jc repl_info
jc repl_create

jc repl_add_ws -id:1 -name:"Sever"
jc repl_add_ws -id:2 -name:"ws filial 2"
jc repl_add_ws -id:3 -name:"ws filial 3"
...
и т.д.


Опционально:

jc repl_snapshot
jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



Настройка рабочей станции
---------------------------------


Обязательно:

jc repl_info
jc repl_create


Опционально:


jc repl_snapshot
jc repl-sync -dir:f:\jdtx-repl\ -mark:true



Разное
---------------------------------


-Xms256m -Xmx1024m