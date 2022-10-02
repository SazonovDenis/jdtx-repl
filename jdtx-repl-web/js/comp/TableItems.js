import {app} from '../app.js';
import {utItems} from '../UtItems.js';

import {itemsPS} from '../data.js';


app.component("tableItems", {
    data() {
        return {
            inp: {
                tableName: "",
                tableNameCheck: false,
                tagsVisible: ["empty", "up", "down", "hidden"]
            },
            items: itemsPS,
            itemsList: null
        }
    },
    created: function() {
        // Развернем дерево items в прямой список itemsList
        this.itemsList = [];
        for (let item of this.items) {
            this.fillPlainList(item, this.itemsList);
        }
        // Заполним (инициализируем) item.tags
        for (let item of this.itemsList) {
            if (item.tags == null) {
                item.tags = [];
            }
        }
        // Заполним (инициализируем) item.parent
        for (let item of this.items) {
            this.fillParentsDesc(item);
        }
        //
        utItems.setItems(this.itemsList);
    },

    methods: {
        fillPlainList(item, plainList) {
            plainList.push(item)
            for (let child of item.childs) {
                this.fillPlainList(child, plainList);
            }
        },

        fillParentsDesc(item) {
            for (let child of item.childs) {
                child.parent = item;
                this.fillParentsDesc(child);
            }
        },

        inp_tableName() {
            //console.info("inp_tableName, value: " + this.inp.tableName);

            //
            for (let item of this.items) {
                this.setMachDesc(item, "", "checked", false)
            }

            //
            this.inp.tableNameCheck = false;
        },
        inp_checkboxClick() {
            //console.info("inp_checkboxClick, value: " + this.inp.tableNameCheck);

            //
            document.getElementById("inp_tableName").focus();

            //
            for (let item of this.items) {
                this.setMachDesc(item, this.inp.tableName, "checked", this.inp.tableNameCheck)
            }
        },

        checked_AttrAdd(attr) {
            //console.info("checked_AttrAdd, attr: " + attr);
            utItems.setItems(this.itemsList);

            for (let item of this.itemsList) {
                if (this.is_match_name(item, this.inp.tableName)) {
                    utItems.itemAttrAdd(item, attr)
                }
            }
        },
        checked_AttrRemove(attr) {
            utItems.setItems(this.itemsList);

            //console.info("checked_AttrRemove, attr: " + attr);
            for (let item of this.itemsList) {
                if (this.is_match_name(item, this.inp.tableName)) {
                    utItems.itemAttrRemove(item, attr)
                }
            }
        },

        setMachDesc(item, str, key, value) {
            if (this.is_match_name(item, str)) {
                item[key] = value;
            }

            for (let child of item.childs) {
                this.setMachDesc(child, str, key, value)
            }
        },

        is_match_name(item, str) {
            return item.name.toUpperCase().includes(str.toUpperCase())
        },
        is_match_tags(item, tagsVisible) {
            if (tagsVisible.includes("empty")) {
                if (item.tags.length == 0) {
                    return true
                }
            }

            if (tagsVisible.includes("hidden")) {
                if (item.tags.includes("hidden")) {
                    return true
                }
            }

            if (tagsVisible.includes("up")) {
                if (item.tags.includes("up")) {
                    return true
                }
            }

            if (tagsVisible.includes("down")) {
                if (item.tags.includes("down")) {
                    return true
                }
            }

            return false;
        }
    },
    template: `
<div class="flex-container">
    <input v-model="inp.tableName" id="inp_tableName" type="text" @keyup="inp_tableName()"/>

    <div class="button button-up" @click="checked_AttrAdd('up')">up: true</div> 
    <div class="button button-down" @click="checked_AttrAdd('down')">down: true</div>
    <div class="button button-hidden" @click="checked_AttrAdd('hidden')">hidden: true</div>

    <div class="button button-up" @click="checked_AttrRemove('up')">up: false</div> 
    <div class="button button-down" @click="checked_AttrRemove('down')">down: false</div>
    <div class="button button-hidden" @click="checked_AttrRemove('hidden')">hidden: false</div>
</div>

<tags-choice :tags=inp.tagsVisible></tags-choice>

<div>
    <div v-for="item in items">
        <table-item :item=item :itemsTree=items :itemsList=itemsList :inp="inp" v-if="is_match_name(item, inp.tableName) && is_match_tags(item, inp.tagsVisible)"></table-item>
    </div>
</div>
`
})
