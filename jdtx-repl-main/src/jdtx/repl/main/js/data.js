// Для взаимодействия
var hub = new Vue({
    el: "#hub_block",
    data: {
        state: {
            result: null,
            error_text: null,
            flash_text: null
        },
        state_dttm: null,
        state_render_helper: {},
    },
    created: function () {
        this.state_render_helper = new Date();
    },
    methods: {
        do_signal: function (signal) {
            console.info("do_signal: " + signal);
            send_post_async("Data/sendSignal?format=alpha-index", this.user_input.signal, on_done_flash);
        }
    }
});

var main = new Vue({
    el: "#main_block",
    data: {
        hub: hub,
        user_input: {
            tableName: "Lic",
            tableSelectedFields: {"NameF": true, "NameO": true},
            itemDoublesSelectedFields: {
                123: {"id": true, "NameF": true},
                3452: {"id": true, "NameO": true, "DocNo": true},
                3712: {"id": true, "NameF": true, "NameI": true, "NameO": true, "DocNo": true},
            }
        },
        tableFields: ["id", "NameF", "NameI", "NameO", "DocNo"],
        itemDoubles: [
            {
                params: {NameF: "F1", NameI: "I1"},
                records: [
                    {id: 123, NameF: "F1", NameI: "I1", DocNo: "L-774913"},
                    {id: 345, NameF: "F1", NameI: "I1", DocNo: "H-568435"}
                ]
            }, {
                params: {NameF: "F2", NameI: "I3"},
                records: [
                    {id: 3452, NameF: "F2", NameI: "I3", DocNo: "L-484854"},
                    {id: 9123, NameF: "F2", NameI: "I3", DocNo: "L-484854"},
                    {id: 3701, NameF: "F2", NameI: "I3", NameO: "O3", DocNo: "H-006965"}
                ]
            }
        ]
    },
    methods: {
        getClass_SelectedField: function (tableField) {
            if (this.user_input.tableSelectedFields[tableField] == true) {
                return "jdx-field-selected";
            }
            return "";
        },
        getClass_SelectedFieldInDouble: function (recordDouble, tableField) {
            var itemDoublesSelectedFieldsThis = this.user_input.itemDoublesSelectedFields[recordDouble.id];
            if (itemDoublesSelectedFieldsThis != null && itemDoublesSelectedFieldsThis[tableField] == true) {
                return "jdx-field-selected";
            }
            return "";
        },
        onClick_Field: function (tableField) {
            if (this.user_input.tableSelectedFields[tableField] == true) {
                Vue.set(this.user_input.tableSelectedFields, tableField, false);
            } else {
                Vue.set(this.user_input.tableSelectedFields, tableField, true);
            }
        },
        /**
         * Выделяем это поле в записи, а в остальных это поле убираем
         */
        onClick_TableField: function (tableDouble, recordDouble, tableField) {
            var itemDoublesSelectedFieldsThis = this.user_input.itemDoublesSelectedFields[recordDouble.id];
            if (itemDoublesSelectedFieldsThis == null) {
                itemDoublesSelectedFieldsThis = {};
                this.user_input.itemDoublesSelectedFields[recordDouble.id] = itemDoublesSelectedFieldsThis;
            }

            if (tableField == "id") {
                // Выделяем всю строку, а в остальных все поля - убираем
                for (var tableField of this.tableFields) {
                    // В остальных это поле убираем
                    for (var tableDoubleRec of tableDouble) {
                        var itemDoublesSelectedFields = this.user_input.itemDoublesSelectedFields[tableDoubleRec.id];
                        if (itemDoublesSelectedFields != null) {
                            itemDoublesSelectedFields[tableField] = false;
                        }
                    }
                    // Выделяем это поле в записи
                    itemDoublesSelectedFieldsThis[tableField] = true;
                    //
                    //this.hub.state_render_helper = new Date();
                }
            } else {
                // В остальных это поле убираем
                for (var tableDoubleRec of tableDouble) {
                    var itemDoublesSelectedFields = this.user_input.itemDoublesSelectedFields[tableDoubleRec.id];
                    if (itemDoublesSelectedFields != null) {
                        itemDoublesSelectedFields[tableField] = false;
                    }
                }
                // Выделяем это поле в записи
                itemDoublesSelectedFieldsThis[tableField] = true;
                //
                //this.hub.state_render_helper = new Date();
            }

            //
            this.$forceUpdate();
        }
    }
});


send_post_async = function (func, data, callback) {
    var xhr = new XMLHttpRequest();

    xhr.open("POST", func, true);
    xhr.setRequestHeader('Content-type', 'application/json; charset=utf-8');

    //
    xhr.onload = function (e) {
        //console.info("xhr.onload");
        if (xhr.readyState === 4) {
            if (xhr.status === 200) {
                callback(xhr.responseText);
            } else {
                console.error("xhr.status != 200: [" + xhr.statusText + "]");
                callback(xhr.statusText);
            }
        }
    };

    xhr.onerror = function (e) {
        console.error("xhr.onerror: [" + xhr.statusText + "]");
        callback(xhr.statusText);
    };

    // Отсылаем объект в формате JSON и с Content-Type application/json
    // Сервер должен уметь такой Content-Type принимать и раскодировать
    xhr.send(data);
};

send_http_async = function (func, params, callback) {
    // console.info("send_http_async");
    // http://localhost:20800/dbm/daoinvoke?daoname=trade.web.main.model.DataDao&daomethod=data&daoparams=[]

    var xhr = new XMLHttpRequest();
    seed = new Date().getTime();
    xhr.open('GET', 'dbm/daoinvoke?seed=' + seed + '&daoname=trade.web.main.model.DataDao&daomethod=' + func + '&daoparams=[]&params=' + params, true);

    //
    xhr.onload = function (e) {
        //console.info("xhr.onload");
        if (xhr.readyState === 4) {
            if (xhr.status === 200) {
                callback(xhr.responseText);
            } else {
                console.error("xhr.status != 200: [" + xhr.statusText + "]");
                callback(xhr.statusText);
            }
        }
    };

    //
    xhr.onerror = function (e) {
        console.error("xhr.onerror: [" + xhr.statusText + "]");
        callback(xhr.statusText);
    };

    //
    xhr.send(null);
};

parseResult = function (data) {
    appCoinList.format = data.format.value;
    appCoinList.tr_portfolio = data.tr_portfolio.value.data;
    for (idx in appCoinList.tr_portfolio) {
        var rec = appCoinList.tr_portfolio[idx];
        if (rec.error_json != null) {
            rec.error = JSON.parse(rec.error_json);
        } else {
            rec.error = null
        }
    }
    appCoinList.tr_orders = data.tr_orders.value.data;
    appCoinList.tr_signals = data.tr_signals.value.data;
    appCoinList.tr_events = data.tr_events.value.data;
    for (idx in appCoinList.tr_events) {
        rec = appCoinList.tr_events[idx];
        if (rec.info_json != null) {
            rec.info = JSON.parse(rec.info_json);
        } else {
            rec.info = null
        }
    }
    appCoinList.balances_last = data.balances_last.value.data;
    appCoinList.stock_tickers_last = data.stock_tickers_last.value.data;
    //
    hub.format = appCoinList.format;
    //
    hub.user_input.bid_ask_dttm = null;
    for (idx in appCoinList.stock_tickers_last) {
        rec = appCoinList.stock_tickers_last[idx];
        if (rec.quote_name == hub.user_input.quote && rec.coin_name == hub.user_input.coin) {
            hub.user_input.bid = rec.bid;
            hub.user_input.ask = rec.ask;
            hub.user_input.bid_ask_dttm = rec.dttm;
            break;
        }
    }
};

doBeforeOnDone = function () {
    // Скроем первичный wait
    wait_first_load_block = document.getElementById("wait_first_load_block");
    wait_first_load_block.style = "display: none";

    // Покажем главный блок (уберем его сокрытие)
    main_block = document.getElementById("main_block");
    if (main_block != null) {
        main_block.style = "";
    }
};

on_done = function (res) {
    // appData.input_data.btn_disabled = false;

    // Скроем первичный wait, покажем главный блок (уберем его сокрытие)
    doBeforeOnDone();


    //
    // todo: раскоменить, когда понадобится отображать факт ожидания ответа от сервера
    //appData.state.state = "done";

    //
    try {
        if (res == "") {
            throw new Error("Ответ от сервера не получен");
        }

        //
        var data = JSON.parse(res);

        //
        if (data.success == false) {
            console.info(data.errors);
            //
            hub.state = {};
            hub.state.result = "error";
            hub.state.error_text = (new Date()).toISOString().replace('T', ' ').substr(0, 19) + ": " + data.errors[0].text;
            hub.state_render_helper = new Date();  // заставляет vue перерисоваться
            //
            return;
        }

        //
        var databox = data.value;
        parseResult(databox);

        //
        hub.state_dttm = new Date();

        //
        hub.state.result = "ok";
        hub.state.error_text = null;
        hub.state_render_helper = new Date();  // заставляет vue перерисоваться
    } catch (e) {
        console.info(e);
        //
        hub.state = {};
        hub.state.result = "error";
        hub.state.error_text = (new Date()).toISOString().replace('T', ' ').substr(0, 19) + ": " + e.message;
        hub.render_helper_value = new Date();  // заставляет vue перерисоваться
        //
    } finally {
        setTimeout("do_reload()", 1000);
    }
};

on_done_flash = function (res) {
    try {
        if (res == "") {
            throw new Error("Ответ от сервера не получен");
        }

        //
        var data = JSON.parse(res);

        //
        if (data.success == false) {
            console.info(data.errors);
            //
            hub.state.flash_result = "error";
            hub.state.flash_text = (new Date()).toISOString().replace('T', ' ').substr(0, 19) + ": " + data.error;
            hub.state_render_helper = new Date();  // заставляет vue перерисоваться
            //
            return;
        }

        //
        hub.state.flash_result = "ok";
        if (data.text == null) {
            hub.state.flash_text = null;
        } else {
            hub.state.flash_text = data.text;
        }
        hub.state_render_helper = new Date();  // заставляет vue перерисоваться
    } catch (e) {
        console.info(e);
        //
        hub.state.flash_text = (new Date()).toISOString().replace('T', ' ').substr(0, 19) + ": " + e.message;
        hub.render_helper_value = new Date();  // заставляет vue перерисоваться
    }
};

do_reload = function () {
    // todo: раскоменить, когда понадобится отображать факт ожидания ответа от сервера
    //appData.state.state = "send";
    //appData.render_helper_value = new Date();  // заставляет vue перерисоваться

    //
    try {
        var res0 = send_http_async("data_xxxxxx", 'stock: xxxx', on_done);
    } catch (e) {
        console.info(e);
    }
};


//
//setTimeout("do_reload()", 1000);