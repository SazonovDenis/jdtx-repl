@echo off



rem 㤠����� ��ண� - �㦡�
call jc repl-service-remove -csc

rem 㤠����� ��ண� - �����
rmdir /Q /S %cd%\web\WEB-INF\data



rem ࠡ��� �⠭�� ""
call jc repl-create -ws:1 -guid:ADC29897BF797F1D.test_dvsa-018FDB43064AF340 -file:"cfg/ws.json"



rem �ࢥ�

rem ������� ������ �������� �㡫���権
call jc repl-set-cfg -cfg:cfg_publications -file:"cfg/publication_up_152_srv.json"



rem ������塞 ࠡ�稥 �⠭樨
call jc repl-add-ws -ws:1 -guid:ADC29897BF797F1D.test_dvsa-018FDB43064AF340 -name:"Server"
call jc repl-add-ws -ws:2 -guid:ADC29897BF797F1D.test_dvsa-021D2F0F9D7FE64D -name:"ws2"
call jc repl-add-ws -ws:3 -guid:ADC29897BF797F1D.test_dvsa-033E684FC1511565 -name:"ws3"



rem ��⨢��㥬 ࠡ�稥 �⠭樨
call jc repl-enable -ws:1
call jc repl-enable -ws:2
call jc repl-enable -ws:3



rem ᮧ���� �騪� ࠡ��� �⠭権
call jc repl-mail-check -create:true



rem �ࠧ� ���뫠�� ����ன�� ��� ��� �⠭権
call jc repl-send-cfg -cfg:cfg_decode -file:"cfg/decode_strategy.json"
call jc repl-send-cfg -ws:1 -cfg:cfg_publications -file:"cfg/publication_up_152_srv.json"
call jc repl-send-cfg -ws:2 -cfg:cfg_publications -file:"cfg/publication_up_152_ws.json"
call jc repl-send-cfg -ws:3 -cfg:cfg_publications -file:"cfg/publication_up_152_ws.json"



rem �ࠧ� ���樨�㥬 䨪��� �������� ��
call jc repl-dbstruct-finish



rem �㦡�
call jc repl-service-install


rem ��� ᢥ�����
call jc repl-info

