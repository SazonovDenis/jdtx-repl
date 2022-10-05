import {app} from '../app.js';
import {utItems} from '../UtItems.js';

import {itemsTBD} from '../data.js';

import './TableItem.js';

app.component("tableItems", {
    props: ["attrLists"],
    data() {
        return {
            inp: {
                tableName: "",
                tableNameCheck: false,
                tagsVisible: ["empty", "up", "down", "none", "hidden"]
            },
            items: itemsTBD,
            itemsInfo: {},
        }
    },
    created: function() {
        utItems.setItems(this.items, this.itemsInfo);
    },

    methods: {
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
            for (let item of utItems.plainList) {
                if (!utItems.attrExists(item, attr) && this.is_match_name(item, this.inp.tableName) && this.is_match_tags(item, this.inp.tagsVisible)) {
                    //console.info("  ".repeat(item.level) + item.name + ", lev: " + item.level);
                    utItems.itemAttrAdd(item, attr)
                }
            }
        },
        checked_AttrRemove(attr) {
            //console.info("checked_AttrRemove, attr: " + attr);
            for (let item of utItems.plainList) {
                if (utItems.attrExists(item, attr) && this.is_match_name(item, this.inp.tableName) && this.is_match_tags(item, this.inp.tagsVisible)) {
                    //console.info("  ".repeat(item.level) + item.name + ", lev: " + item.level);
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

            if (tagsVisible.includes("none")) {
                if (item.tags.includes("none")) {
                    return true
                }
            }

            return false;
        }
    },
    template: `
<div class="flex-container">
    <input v-model="inp.tableName" id="inp_tableName" type="text" @keyup="inp_tableName()"/>

    <div class="button button-tag-up" @click="checked_AttrAdd('up')">up: true</div> 
    <div class="button button-tag-down" @click="checked_AttrAdd('down')">down: true</div>
    <div class="button button-tag-none" @click="checked_AttrAdd('none')">none: true</div>
    <div class="button button-tag-hidden" @click="checked_AttrAdd('hidden')">hidden: true</div>

    <div class="button button-tag-up" @click="checked_AttrRemove('up')">up: false</div> 
    <div class="button button-tag-down" @click="checked_AttrRemove('down')">down: false</div>
    <div class="button button-tag-none" @click="checked_AttrRemove('none')">none: false</div>
    <div class="button button-tag-hidden" @click="checked_AttrRemove('hidden')">hidden: false</div>
</div>

<tags-choice :tags=inp.tagsVisible :itemsInfo="itemsInfo"></tags-choice>


<div class="table-items">
    <div v-for="item in items">
        <table-item :item=item :inp="inp" v-if="is_match_name(item, inp.tableName) && is_match_tags(item, inp.tagsVisible)"></table-item>
    </div>
</div>


<div>
    <button>Ok</button>
</div>
`
})
