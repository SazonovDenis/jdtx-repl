## Общее


Переименовать sample.* в папке WEB-INF

sample._app.rt

sample._db-ini.rt

sample.log.properties



Переименовать cfg/sample.* в папке WEB-INF/cfg

cfg/sample.srv.json

cfg/sample.ws.json



## Настройка почтового сервера


Обязательно:

Создать структуру почтовых каталогов, опираясь на guid из cfg/srv.json

ov repl_check_mail -ws:1 -create:true

ov repl_check_mail -ws:2 -create:true

ov repl_check_mail -ws:3 -create:true

...

и т.д.



## Настройка БД сервера (как рабочей станции)


#### Обязательно:


В файле _app.rt включить рабочую станцию (bgtask name="ws" enabled="true")


Создать репликационные структры с указанием кода станции:

jc repl-info

jc repl-create -id:1



#### Опционально:


jc repl-snapshot

jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



## Настройка БД сервера (как сервера)


#### Обязательно:

В файле _app.rt включить сервер (bgtask name="server" enabled="true") 


Добавить все рабочие станции:

jc repl-add-ws -id:1 -name:"Sever"

jc repl-add-ws -id:2 -name:"ws filial 2"

jc repl-add-ws -id:3 -name:"ws filial 3"

...

и т.д.


Вновь добавленные станции нужно активировать:

jc repl-enable -ws:1

jc repl-enable -ws:2

jc repl-enable -ws:3

...

и т.д.




## Настройка БД рабочей станции




#### Обязательно:

В файле _app.rt отключить сервер (bgtask name="server" enabled="false"), стереть конфиг srv.json

В файле _app.rt включить рабочую станцию (bgtask name="ws" enabled="true")



Создать репликационные структры с указанием кода станции:

jc repl-info

jc repl-create -id:XXX


#### Опционально:

jc repl-snapshot

jc repl-sync -dir:f:\jdtx-repl\ -mark:true



## Запуск


jc run



## Разное


Добавить в запуск java:

-Xms256m -Xmx1024m