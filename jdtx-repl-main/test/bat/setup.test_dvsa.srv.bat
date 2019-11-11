@echo off



rem удаление старого - служба
call jc repl-service-remove -csc

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data



rem рабочая станция ""
call jc repl-create -ws:1 -guid:ADC29897BF797F1D.test_dvsa-018FDB43064AF340 -file:"cfg/ws.json"



rem сервер

rem напрямую задаем структуру публикаций
call jc repl-set-cfg -cfg:cfg_publications -file:"cfg/publication_up_152_srv.json"



rem добавляем рабочие станции
call jc repl-add-ws -ws:1 -guid:ADC29897BF797F1D.test_dvsa-018FDB43064AF340 -name:"Server"
call jc repl-add-ws -ws:2 -guid:ADC29897BF797F1D.test_dvsa-021D2F0F9D7FE64D -name:"ws2"
call jc repl-add-ws -ws:3 -guid:ADC29897BF797F1D.test_dvsa-033E684FC1511565 -name:"ws3"



rem активируем рабочие станции
call jc repl-enable -ws:1
call jc repl-enable -ws:2
call jc repl-enable -ws:3



rem создаем ящики рабочих станций
call jc repl-mail-check -create:true



rem сразу рассылаем настройки для всех станций
call jc repl-send-cfg -cfg:cfg_decode -file:"cfg/decode_strategy.json"
call jc repl-send-cfg -ws:1 -cfg:cfg_publications -file:"cfg/publication_up_152_srv.json"
call jc repl-send-cfg -ws:2 -cfg:cfg_publications -file:"cfg/publication_up_152_ws.json"
call jc repl-send-cfg -ws:3 -cfg:cfg_publications -file:"cfg/publication_up_152_ws.json"



rem сразу инициируем фиксацию структуры БД
call jc repl-dbstruct-finish



rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

