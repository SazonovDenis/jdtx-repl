export {utItems}


const utItems = {
    plainList: [],

    itemIndex: new Map(),

    attrLists: {
        up: new Set(),
        down: new Set(),
        none: new Set(),
        hidden: new Set(),
        empty: new Set(),
    },


    itemsInfo: {},

    setItems(items, itemsInfo) {
        // Тут выходные данные модели
        this.itemsInfo = itemsInfo;
        this.itemsInfo["up"] = {};
        this.itemsInfo["down"] = {};
        this.itemsInfo["none"] = {};
        this.itemsInfo["hidden"] = {};
        this.itemsInfo["empty"] = {};

        // Развернем дерево items в прямой список plainList
        this.plainList = [];
        for (let item of items) {
            this.fillPlainList(item);
        }

        // Заполним (инициализируем) item.parent
        for (let item of items) {
            this.fillParentsDesc(item);
        }

        // Заполним (инициализируем) item.tags
        for (let item of this.plainList) {
            if (item.tags == null) {
                item.tags = [];
            }
        }

        // Заполним itemIndex
        for (let item of this.plainList) {
            let items = this.itemIndex.get(item.name);
            if (items == null) {
                items = [];
                this.itemIndex.set(item.name, items);
            }
            items.push(item);
        }

        // Заполним (инициализируем) attrLists["empty"]
        let attrList = this.attrLists["empty"];
        for (let item of this.plainList) {
            attrList.add(item.name);
        }

        // Отчитаемся
        this.calcOutData();
    },
    fillPlainList(item) {
        this.plainList.push(item)
        for (let child of item.childs) {
            this.fillPlainList(child);
        }
    },
    fillParentsDesc(item) {
        for (let child of item.childs) {
            child.parent = item;
            this.fillParentsDesc(child);
        }
    },
    calcOutData() {
        this.itemsInfo["up"].size = this.attrLists["up"].size;
        this.itemsInfo["down"].size = this.attrLists["down"].size;
        this.itemsInfo["none"].size = this.attrLists["none"].size;
        this.itemsInfo["hidden"].size = this.attrLists["hidden"].size;
        this.itemsInfo["empty"].size = this.attrLists["empty"].size;
    },


    attrExists(item, attr) {
        return item.tags.includes(attr)
    },
    attrRemove(item, attr) {
        let index = item.tags.indexOf(attr);
        if (index > -1) {
            item.tags.splice(index, 1);
        }
        //
        let attrList = this.attrLists[attr];
        attrList.delete(item.name);
        //
        if (item.tags.length == 0) {
            attrList = this.attrLists["empty"];
            attrList.add(item.name);
        }
        // Отчитаемся
        this.calcOutData();
    },
    attrAdd(item, attr) {
        if (!this.attrExists(item, attr)) {
            item.tags.push(attr);
        }
        //
        let attrList = this.attrLists[attr];
        attrList.add(item.name);
        //
        attrList = this.attrLists["empty"];
        attrList.delete(item.name);
        // Отчитаемся
        this.calcOutData();
    },

    itemAttrRemove(item, attr) {
        if (attr == "hidden" || attr == "none") {
            // При удалении атрибута hidden или none - удалим у всех потомков
            utItems.attrRemoveDesc(item, attr);
        } else {
            // При удалении атрибута up или down - удалим у всех родителей
            utItems.attrRemoveParents(item, attr);
        }
    },
    itemAttrAdd(item, attr) {
        if (attr == "hidden" || attr == "none") {
            // При добавлении атрибута hidden или none - добавим у всех родителей
            utItems.attrAddParents(item, attr);

            // При добавлении атрибута hidden или none - удалим атрибуты up и down у всех родителей
            utItems.attrRemoveParents(item, "up");
            utItems.attrRemoveParents(item, "down");
            if (attr == "hidden") {
                utItems.attrRemoveParents(item, "none");
            } else {
                utItems.attrRemoveParents(item, "hidden");
            }
        } else {
            // При добавлении атрибута up или down - добавим у всех потомков
            utItems.attrAddDesc(item, attr);

            // При добавлении атрибута up или down - удалим атрибуты hidden и none у всех родителей
            utItems.attrRemoveDesc(item, "none");
            utItems.attrRemoveDesc(item, "hidden");
        }
    },

    attrRemoveDesc(item, attr) {
        // Себя
        this.attrRemove(item, attr)

        // Своих однофамильцев
        this.attrRemoveByName(item.name, attr);

        // Своих потомков
        for (let child of item.childs) {
            this.attrRemoveDesc(child, attr)
        }
    },

    attrRemoveParents(item, attr) {
        const set = new Set();
        this.attrRemoveParents_(item, attr, set)
    },

    attrRemoveParents_(item, attr, set) {
        if (set.has(item)) {
            return;
        }
        set.add(item)

        // Себя
        this.attrRemove(item, attr)

        // Своего предка
        if (item.parent != null) {
            this.attrRemoveParents_(item.parent, attr, set)
        }

        // Своих однофамильцев и их предков
        let items = this.itemIndex.get(item.name);
        for (let itemNamesake of items) {
            //for (let itemNamesake of this.plainList) {
            if (/*itemNamesake.name == item.name &&*/ !set.has(itemNamesake)) {
                this.attrRemove(itemNamesake, attr);
                if (itemNamesake.parent != null) {
                    this.attrRemoveParents_(itemNamesake.parent, attr, set);
                }
            }
        }
    },

    attrAddDesc(item, attr) {
        // Себя
        this.attrAdd(item, attr)

        // Своих однофамильцев
        this.attrAddByName(item.name, attr);

        // Своих потомков
        for (let child of item.childs) {
            this.attrAddDesc(child, attr)
        }
    },

    attrAddParents(item, attr) {
        const set = new Set();
        this.attrAddParents_(item, attr, set)
    },
    attrAddParents_(item, attr, set) {
        if (set.has(item)) {
            return;
        }
        set.add(item)

        // Себя
        this.attrAdd(item, attr)

        // Своего предка
        if (item.parent != null) {
            this.attrAddParents_(item.parent, attr, set)
        }

        // Своих однофамильцев и их предков
        let items = this.itemIndex.get(item.name);
        for (let itemNamesake of items) {
            //for (let itemNamesake of this.plainList) {
            if (/*itemNamesake.name == item.name &&*/ !set.has(itemNamesake)) {
                this.attrAdd(itemNamesake, attr);
                if (itemNamesake.parent != null) {
                    this.attrAddParents_(itemNamesake.parent, attr, set)
                }
            }
        }
    },

    attrAddByName(itemName, attr) {
        //console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        //for (let item of this.plainList) {
        let items = this.itemIndex.get(itemName);
        for (let item of items) {
            //if (item.name == itemName) {
            this.attrAdd(item, attr);
            //}
        }
    },

    attrRemoveByName(itemName, attr) {
        //console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        //for (let item of this.plainList) {
        let items = this.itemIndex.get(itemName);
        for (let item of items) {
            //if (item.name == itemName) {
            this.attrRemove(item, attr);
            //}
        }
    },

    setValueDesc(item, key, value) {
        //console.info("setValueDesc, item: " + item.name + ", [" + key + "] <- " + value);

        // Себя
        item[key] = value;

        // Своих однофамильцев
        this.setValueByName(item.name, key, value);

        // Своих потомков
        for (let child of item.childs) {
            this.setValueDesc(child, key, value);
        }
    },

    setValueByName(itemName, key, value) {
        //console.info("setValueByName, itemName: " + itemName + ", [" + key + "] <- " + value);

        // Все элементы c таким именем
        //for (let item of this.plainList) {
        let items = this.itemIndex.get(itemName);
        for (let item of items) {
            //if (item.name == itemName) {
            item[key] = value;
            //}
        }
    },
}