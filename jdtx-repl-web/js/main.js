export {main1_f1};


import {app} from './app.js';
import {itemsAll, itemsLic} from './data.js';

//
import './comp/TagsChoice.js';
import './comp/TableItem.js';
import './comp/TableItems.js';

//
import {module1_f1} from './module1.js';


function main1_f1() {
    console.info("=== function main1_f1 ===");
    module1_f1();
}

console.info("=== module main1 ===");

function button1_click() {
    console.info("=== function button1_click ===");
    console.info(itemsAll);
}

function button2_click() {
    console.info("=== function button2_click ===");
    console.info(itemsLic);
}

let button1 = document.getElementById("button1");
button1.onclick = button1_click;

let button2 = document.getElementById("button2");
button2.onclick = button2_click;




//
app.mount('#app')
