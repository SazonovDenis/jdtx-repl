@echo off



rem удаление старого - служба
call jc repl-service-remove -csc

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data



rem рабочая станция "ws2"
call jc repl-create -ws:2 -guid:ADC29897BF797F1D.test_dvsa-021D2F0F9D7FE64D -file:"cfg/ws.json"



rem служба
call jc repl-service-install



rem для сведения
call jc repl-info

