import {app} from '../app.js';
import {utItems} from '../UtItems.js';


app.component("tableItem", {
    props: ['item', "inp"],
    created: function() {
        // Чтобы использовать имя "utItems" в шаблоне - иначе шаблон не видит!
        this.utItems = utItems;
    },
    methods: {
        itemClick(item) {
            item.expanded = !item.expanded;
            for (let child of item.childs) {
                child.visible = item.expanded;
            }
        },
        itemAttrClick(item, attr) {
            //console.info("itemAttrClick, name: " + item.name + ", attr: " + attr);
            if (utItems.attrExists(item, attr)) {
                utItems.itemAttrRemove(item, attr)
            } else {
                utItems.itemAttrAdd(item, attr)
            }
        },

        getNameWrapped(item, str) {
            let s1 = str.toUpperCase();
            let s2 = "<span class='selected'>" + str.toUpperCase() + "</span>";
            if (s1 == "") {
                var s = item.name;
            } else {
                var s = item.name.replaceAll(s1, s2);
            }
            return s;
        },
    },
    template: `
<div v-if="item.visible">
    <span :style="'padding-left: ' + item.level*15 + 'px'"/>
    
    <span v-if="item.childsCount == 0" @click="itemClick(item)">&nbsp;&nbsp;</span>
    <span v-if="item.childsCount != 0" @click="itemClick(item)"><span v-if="item.expanded">&times;</span><span v-if="!item.expanded">&plus;</span></span>
    
    <span :class="'inp-tag inp-tag-up inp-tag-up-' + utItems.attrExists(item, 'up')" @click="itemAttrClick(item, 'up')"><span v-if="utItems.attrExists(item, 'up')">&nbsp;&uarr;&nbsp;</span><span v-if="!utItems.attrExists(item, 'up')">&nbsp;</span></span>
    <span :class="'inp-tag inp-tag-down inp-tag-down-' + utItems.attrExists(item, 'down')" @click="itemAttrClick(item, 'down')"><span v-if="utItems.attrExists(item, 'down')">&nbsp;&darr;&nbsp;</span><span v-if="!utItems.attrExists(item, 'down')">&nbsp;</span></span>
    <span :class="'inp-tag inp-tag-none inp-tag-none-' + utItems.attrExists(item, 'none')" @click="itemAttrClick(item, 'none')"><span v-if="utItems.attrExists(item, 'none')">&nbsp;&ndash;&nbsp;</span><span v-if="!utItems.attrExists(item, 'none')">&nbsp;</span></span>
    <span :class="'inp-tag inp-tag-hidden inp-tag-hidden-' + utItems.attrExists(item, 'hidden')" @click="itemAttrClick(item, 'hidden')"><span v-if="utItems.attrExists(item, 'hidden')">&nbsp;&times;&nbsp;</span><span v-if="!utItems.attrExists(item, 'hidden')">&nbsp;</span></span>
    
    <span @click="itemClick(item)">
        <span v-html="getNameWrapped(item, inp.tableName)"/><span v-if="item.childsCountFull != 0"> ({{item.childsCountFull}})</span>
    </span>
    <span v-if="item.recursive">&#x27F3;</span>
    <div v-for="item in item.childs">
        <table-item :item=item :inp="inp"></table-item>
    </div>
</div>
`
})
