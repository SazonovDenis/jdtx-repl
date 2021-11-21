## Руководство пользователя по смене структуры БД

#### Шаги

- Переход в "режим молчания". На сервере выполняется команда `jc repl-dbstruct-start`.  
- Ожидание, что все станции перешли в "режим молчания". На сервере проверяется команда `jc repl-dbstruct-state -wait`.
  Запоминаем maxAge. 
- Повторная отправка команды MUTE. На сервере выполняется команда `jc repl-ws-mute -ws:0`
  Ожидание, что minAge по всем станциям станет больше ранее запомненного maxAge. На сервере проверяется команда `jc repl-dbstruct-state`
- Опционально - разрешение серверной базе разослать обновления ПО. На сервере выполняется команда `jc repl_ws_unmute -ws:1`
- Выполняется смена структуры БД сервера (вручную или обновлением прикладного ПО).
- Если в структуре изменился состав таблиц, то для сервера напрямую задаем структуру публикаций. 
  На сервере выполняется команда `repl_set_cfg -cfg:cfg_publications -file:...`, 
  в качестве параметра `-file` указываем json-файл с новыми правилами публикаций.

  Для сервера напрямую задаем стратегию перекодировок. 
- Рассылаем на рабочие станции новые правила публикаций. 
  На сервере выполняется команда `repl_send_cfg -cfg:cfg_publications -file:... -ws:...`,
  в качестве параметра `-file` указываем json-файл с новыми правилами публикаций. 

  Для рабочих станций напрямую задаем стратегию перекодировок. 

  **Важно!** Команда repl_send_cfg должна быть адресована конкретным станциям (кроме серверной базы),
  широковещательная команда не применяется! 
- Выполняется реальная смена структуры БД на рабочих станциях (вручную или обновлением прикладного ПО). 
- Ожидаем завершения смены версии БД на рабочих станциях.
- Фиксация изменения структуры и выход репликационной сети из "режима молчания". 
  На сервере выполняется команда `jc repl-dbstruct-finish`. 

##### Для справки

- Просмтор состояния "режима молчания" - команда `jc repl-dbstruct-state`.

#### Пример:

Смена структуры вместе с рассылкой обновления прикладного ПО.

~~~ bat
rem Переход в "режим молчания"
jc repl-dbstruct-start

rem Ожидание, что все станции перешли в "режим молчания" 
jc repl-dbstruct-state -wait

rem Запоминаем maxAge 

rem Повторная отправка команды MUTE  
jc repl-ws-mute -ws:all

rem Ожидание, что minAge по всем станциям станет больше ранее запомненного maxAge 
jc repl-dbstruct-state 

rem Разрешение серверной базе разослать обновления ПО
jc repl_ws_unmute -ws:1

rem Выкладывание обновления прикладного ПО в серверную БД, чтобы оно разошлось по репликации
pause Отправьте обновления прикладного ПО в серверную БД

rem Обновляем прикладное ПО на сервере, меняем на сервере структуру БД
pause Смените структуру серверной базы (обновите прикладное ПО, запустите, дождитесь смены структуры БД сервера)

rem Для сервера напрямую задаем структуру публикаций
jc repl_set_cfg -cfg:cfg_decode -file:cfg/decode_strategy_186.json
jc repl_set_cfg -cfg:cfg_publications -file:cfg/publication_lic_186_srv.json

rem Рассылаем на рабочие станции новую стратегию перекодировки
jc repl_send_cfg -cfg:cfg_decode -file:cfg/decode_strategy_186.json -ws:2
jc repl_send_cfg -cfg:cfg_decode -file:cfg/decode_strategy_186.json -ws:3
jc repl_send_cfg -cfg:cfg_decode -file:cfg/decode_strategy_186.json -ws:4
jc repl_send_cfg -cfg:cfg_decode -file:cfg/decode_strategy_186.json -ws:5

rem Рассылаем на рабочие станции новые правила публикаций
jc repl_send_cfg -cfg:cfg_publications -file:cfg/publication_lic_186_ws.json -ws:2
jc repl_send_cfg -cfg:cfg_publications -file:cfg/publication_lic_186_ws.json -ws:3
jc repl_send_cfg -cfg:cfg_publications -file:cfg/publication_lic_186_ws.json -ws:4
jc repl_send_cfg -cfg:cfg_publications -file:cfg/publication_lic_186_ws.json -ws:5

rem Завершение изменения структуры
jc repl-dbstruct-finish
~~~

Как вариант

~~~
rem Рассылаем настройки для всех станций
call jc repl-send-cfg -cfg:cfg_decode -file:"cfg/decode_strategy_173.json"
call jc repl-send-cfg -cfg:cfg_publications -file:"cfg/publication_lic_173_ws.json"

rem Рассылаем особую настройку для сервера (предыдущая широковещательная рассылка тем самым перекрывается)
call jc repl-send-cfg -cfg:cfg_publications -file:"cfg/publication_lic_173_srv.json" -ws:1
~~~
