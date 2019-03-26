## Настройка сервера 


В файле _app.rt включить сервер (bgtask name="server" enabled="true") 

В каталоге web\WEB-INF\cfg\ переименовать sample.srv.json в srv.json

В файле web\WEB-INF\cfg\ws.json выбрать: publication_full_152 или publication_full


>setup.***.srv.bat

Переименовать run.bat_ в run.bat и запустить его



## Настройка рабочей станции


В файле web\WEB-INF\cfg\ws.json выбрать: publication_full_152 или publication_full

В файле setup.***.ws.bat раскоментить нужную станцию

>setup.***.ws.bat

Переименовать run.bat_ в run.bat и запустить его
