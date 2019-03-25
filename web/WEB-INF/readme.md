## Настройка БД сервера (как рабочей станции)


#### Обязательно:


Создать репликационные структры с указанием кода станции:

>jc repl-create -ws:1 -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
                           



#### Опционально:


Выгрузить все данные на сервер:

>jc repl-snapshot


Отправить данные через флешку:

>jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



## Настройка БД сервера (как сервера)


#### Обязательно:


В файле _app.rt включить сервер (bgtask name="server" enabled="true") 


Добавить все рабочие станции:

>jc repl-add-ws -ws:1 -name:"Sever" -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX

>jc repl-add-ws -ws:2 -name:"ws filial 2" -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX

>jc repl-add-ws -ws:3 -name:"ws filial 3" -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX

...

и т.д.


Вновь добавленные станции нужно активировать:

>jc repl-enable -ws:1

>jc repl-enable -ws:2

>jc repl-enable -ws:3

...

и т.д.



#### Опционально:


Создать структуру почтовых каталогов, опираясь на guid:

>jc repl-mail-check -create:true -ws:1

>jc repl-mail-check -create:true -ws:2

>jc repl-mail-check -create:true -ws:3

...

и т.д.



## Настройка БД рабочей станции




#### Обязательно:



Создать репликационные структры с указанием кода станции:

>jc repl-create -ws:XXX -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
                           


Проверть репликационные структры:

>jc repl-info



#### Опционально:


Выгрузить все данные на сервер:

>jc repl-snapshot


Отправить данные через флешку:

>jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true




## Запуск


>jc run



## Разное


Добавить в запуск java:

>-Xms256m -Xmx1024m



Проверть репликационные структры: 

>jc repl-info 