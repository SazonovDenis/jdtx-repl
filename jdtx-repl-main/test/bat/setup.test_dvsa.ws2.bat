@echo off



rem 㤠����� ��ண� - �㦡�
call jc repl-service-remove -csc

rem 㤠����� ��ண� - �����
rmdir /Q /S %cd%\web\WEB-INF\data



rem ࠡ��� �⠭�� "ws2"
call jc repl-create -ws:2 -guid:ADC29897BF797F1D.test_dvsa-021D2F0F9D7FE64D -file:"cfg/ws.json"



rem �㦡�
call jc repl-service-install



rem ��� ᢥ�����
call jc repl-info

