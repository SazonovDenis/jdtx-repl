Общее
---------------------------------


Переименовать sample.* в корень WEB-INF
sample._app.rt
sample._db-ini.rt
sample.log.properties


Переименовать cfg/sample.* в WEB-INF/cfg
cfg/sample.srv.json
cfg/sample.ws.json



Настройка сервера
---------------------------------


Обязательно:

jc repl_info
jc repl_create

jc repl_add_ws -name:"ws_filial 1"
jc repl_add_ws -name:"ws_filial 2"
...
и т.д.


Опционально:

jc repl_setup



Настройка рабочей станции
---------------------------------


Обязательно:

jc repl_info
jc repl_create


Опционально:

jc repl_setup



Разное
---------------------------------


-Xms256m -Xmx1024m