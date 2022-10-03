import {app} from '../app.js';
import {utItems} from "../UtItems.js"


app.component("tagsChoice", {
    props: ["tags", "itemsInfo"],
    data() {
        return {
            tagsAll: [],
        }
    },
    created: function() {
        for (let tag of this.tags) {
            this.tagsAll.push(tag);
        }
        //app.set(utItems.attrLists.hidden, "hidden", this.attrLists.hidden);
        //this.attrLists = Vue.reactive(utItems.attrLists);
    },
    methods: {
        tagClick(tag) {
            let index = this.tags.indexOf(tag);
            if (index > -1) {
                this.tags.splice(index, 1);
            } else {
                this.tags.push(tag);
            }
            //console.info("this.tags: " + this.tags);
        }
    },
    template: `
<div class="flex-container">
    <div v-for="tag in tagsAll">
        <div :class="'tag-element tag-' + tag + ' ' + 'tag-' + tags.includes(tag)" @click="tagClick(tag)">{{tag}} {{itemsInfo[tag].size}}</div> 
    </div>
</div>
`
})



