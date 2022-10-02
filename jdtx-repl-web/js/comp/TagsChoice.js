import {app} from '../app.js';


app.component("tagsChoice", {
    props: ["tags"],
    data() {
        return {
            tagsAll: []
        }
    },
    created: function() {
        for (let tag of this.tags) {
            this.tagsAll.push(tag);
        }
    },
    methods: {
        tagClick(tag) {
            let index = this.tags.indexOf(tag);
            if (index > -1) {
                this.tags.splice(index, 1);
            } else {
                this.tags.push(tag);
            }
            console.info("this.tags: " + this.tags);
        }
    },
    template: `
<div class="flex-container">
    <div v-for="tag in tagsAll">
        <div :class="'tag-button tag-' + tag + ' ' + 'tag-' + tags.includes(tag)" @click="tagClick(tag)">{{tag}}</div> 
    </div>
</div>
`
})



