@echo off

rem �㦡�
call jc repl-service-remove -csc


rem ࠡ��� �⠭��
call jc repl-create -ws:1 -guid:413DF2C54CC9D2A2.test_dvsa-014CEC090421C3AC


rem �ࢥ�


call jc repl-add-ws -ws:1 -guid:413DF2C54CC9D2A2.test_dvsa-014CEC090421C3AC -name:"Server"
call jc repl-add-ws -ws:2 -guid:413DF2C54CC9D2A2.test_dvsa-02E4A13756A63160 -name:"ws2"
call jc repl-add-ws -ws:3 -guid:413DF2C54CC9D2A2.test_dvsa-030A2C4C0B819CA3 -name:"ws3"

call jc repl-enable -ws:1
call jc repl-enable -ws:2
call jc repl-enable -ws:3


rem ���樠������ �������� ��
call jc repl-dbstruct-finish


rem ����
call jc repl-mail-check -create:true


rem �㦡�
call jc repl-service-install


rem ��� ᢥ�����
call jc repl-info

