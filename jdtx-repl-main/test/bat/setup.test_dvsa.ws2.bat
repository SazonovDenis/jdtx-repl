@echo off 

rem �㦡�
call jc repl-service-remove -csc


rem ࠡ��� �⠭�� "ws2"
call jc repl-create -ws:2 -guid:413DF2C54CC9D2A2.test_dvsa-02E4A13756A63160


rem �㦡�
call jc repl-service-install


rem ��� ᢥ�����
call jc repl-info

