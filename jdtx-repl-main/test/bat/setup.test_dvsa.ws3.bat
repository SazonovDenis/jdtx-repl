@echo off 

rem �㦡�
call jc repl-service-remove -csc


rem ࠡ��� �⠭�� "ws3"
call jc repl-create -ws:3 -guid:413DF2C54CC9D2A2.test_dvsa-030A2C4C0B819CA3


rem �㦡�
call jc repl-service-install


rem ��� ᢥ�����
call jc repl-info

