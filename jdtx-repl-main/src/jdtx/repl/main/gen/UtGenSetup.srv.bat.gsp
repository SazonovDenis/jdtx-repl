<%@ page import="jdtx.repl.main.api.manager.CfgType" %>
@echo off

call jc -csc



rem удаление старого - служба
call jc repl-service-remove -all

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data



rem рабочая станция "${args.ws_name}"
call jc repl-create -ws:${args.ws_list[0].ws_no} -guid:${args.repl_guid}-${args.ws_list[0].ws_guid} -file:"cfg/ws.json"



rem сервер

rem напрямую задаем структуру публикаций
call jc repl-set-cfg -cfg:${CfgType.PUBLICATIONS} -file:"cfg/publication_lic_194_srv.json"



rem добавляем рабочие станции
<% for (int i = 0; i < args.ws_list.size; i++) { %>
<% if (args.ws_list[i].ws_no==1)  { %>
call jc repl-add-ws -ws:${args.ws_list[i].ws_no} -guid:${args.repl_guid}-${args.ws_list[i].ws_guid} -cfg_publications:"cfg/publication_lic_194_srv.json" -cfg_decode:"cfg/decode_strategy_194.json" -name:"${args.ws_list[i].ws_name}"
<% } else  { %>
call jc repl-add-ws -ws:${args.ws_list[i].ws_no} -guid:${args.repl_guid}-${args.ws_list[i].ws_guid} -cfg_publications:"cfg/publication_lic_194_ws.json" -cfg_decode:"cfg/decode_strategy_194.json" -name:"${args.ws_list[i].ws_name}"
<% } %>
<% } %>



rem активируем рабочие станции
<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-ws-enable -ws:${args.ws_list[i].ws_no}
<% } %>



rem создаем ящики рабочих станций
call jc repl-mail-check -create:true



rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

