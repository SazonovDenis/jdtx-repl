<% for (sss in args.p2) { %>
  p2: ${sss}
<% } %>

@echo off
call jc repl-service-remove



<% int n1=0 %>
rem рабочая станция
call jc repl-create -ws:${n1+1} -guid:${args.guid_repl}-${args.ws_list[n1].guid}



rem сервер


<% for (int no=0; no < args.ws_list.size; no++) { %>
call jc repl-add-ws -ws:${no+1} -guid:${args.guid_repl}-${args.ws_list[no].guid} -name:"${args.ws_list[no].name}"
<% } %>

<% for (int no=0; no < args.ws_list.size; no++) { %>
call jc repl-enable -ws:${no+1}
<% } %>



rem почта
call jc repl-mail-check -create:true



rem служба
call jc repl-service-install



rem для сведения
call jc repl-info

