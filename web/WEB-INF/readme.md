## Настройка БД сервера



#### Обязательно:

	
В каталоге web\WEB-INF\cfg\ переименовать sample.srv.json в srv.json

В файле web\WEB-INF\cfg\ws.json выбрать publication_full_152 или publication_full

В файле _app.rt включить сервер (bgtask name="server" enabled="true") 


Создать репликационные структры с указанием кода станции:

>jc repl-create -ws:1 -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
                           


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



Создать структуру почтовых каталогов:

>jc repl-mail-check -create:true

...

и т.д.


Выгрузить все данные (для сервера):

>jc repl-snapshot


Отправить данные через флешку:

>jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true





## Настройка рабочей станции



#### Обязательно:


Создать репликационные структры с указанием кода станции:

>jc repl-create -ws:XXX -guid:XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
                           



#### Опционально:


Выгрузить все данные на сервер:

>jc repl-snapshot


Отправить данные через флешку:

>jc repl-sync-srv -dir:f:\jdtx-repl\ -mark:true



## Разное


Запуск

>jc run


Добавить в опции запуска java:

>-Xms256m -Xmx1024m



Проверть репликационные структры: 

>jc repl-info 

