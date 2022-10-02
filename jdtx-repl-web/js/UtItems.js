export {utItems}


const utItems = {
    itemsList: [],

    setItems(items){
        this.itemsList = items;
    },

    attrExists(item, attr) {
        return item.tags.includes(attr)
    },
    attrRemove(item, attr) {
        let index = item.tags.indexOf(attr);
        if (index > -1) {
            item.tags.splice(index, 1);
        }
    },
    attrAdd(item, attr) {
        if (!this.attrExists(item, attr)) {
            item.tags.push(attr);
        }
    },

    itemAttrRemove(item, attr) {
        if (attr == "hidden") {
            // При удалении родителя - удалим всех потомков
            utItems.attrRemoveDesc(item, attr, this.itemsList);
        } else {
            // При удалении потомка - удалим всех родителей
            utItems.attrRemoveParents(item, attr, this.itemsList);
        }
    },
    itemAttrAdd(item, attr) {
        if (attr == "hidden") {
            // При добавлении потомка - добавим всех родителей
            utItems.attrAddParents(item, attr, this.itemsList);
            utItems.attrRemoveParents(item, "up", this.itemsList);
            utItems.attrRemoveParents(item, "down", this.itemsList);
        } else {
            // При добавлении родителя - добавим всех потомков
            utItems.attrAddDesc(item, attr, this.itemsList);
            utItems.attrRemoveDesc(item, "hidden", this.itemsList);
        }
    },

    attrRemoveDesc(item, attr, items) {
        // Себя
        this.attrRemove(item, attr)

        // Своих однофамильцев
        this.attrRemoveByName(item.name, attr, items);

        // Своих потомков
        for (let child of item.childs) {
            this.attrRemoveDesc(child, attr, items)
        }
    },

    attrRemoveParents(item, attr, items) {
        const set = new Set();
        this.attrRemoveParents_(item, attr, items, set)
    },

    attrRemoveParents_(item, attr, items, set) {
        if (set.has(item)) {
            return;
        }
        set.add(item)

        // Себя
        this.attrRemove(item, attr)

        // Своего предка
        if (item.parent != null) {
            this.attrRemoveParents_(item.parent, attr, items, set)
        }

        // Своих однофамильцев и их предков
        for (let itemNamesake of items) {
            if (itemNamesake.name == item.name && !set.has(itemNamesake)) {
                this.attrRemove(itemNamesake, attr);
                if (itemNamesake.parent != null) {
                    this.attrRemoveParents_(itemNamesake.parent, attr, items, set);
                }
            }
        }
    },

    attrAddDesc(item, attr, items) {
        // Себя
        this.attrAdd(item, attr)

        // Своих однофамильцев
        this.attrAddByName(item.name, attr, items);

        // Своих потомков
        for (let child of item.childs) {
            this.attrAddDesc(child, attr, items)
        }
    },

    attrAddParents(item, attr, items) {
        const set = new Set();
        this.attrAddParents_(item, attr, items, set)
    },
    attrAddParents_(item, attr, items, set) {
        if (set.has(item)) {
            return;
        }
        set.add(item)

        // Себя
        this.attrAdd(item, attr)

        // Своего предка
        if (item.parent != null) {
            this.attrAddParents_(item.parent, attr, items, set)
        }

        // Своих однофамильцев и их предков
        for (let itemNamesake of items) {
            if (itemNamesake.name == item.name && !set.has(itemNamesake)) {
                this.attrAdd(itemNamesake, attr);
                if (itemNamesake.parent != null) {
                    this.attrAddParents_(itemNamesake.parent, attr, items, set)
                }
            }
        }
    },

    attrAddByName(itemName, attr, items) {
        //console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                this.attrAdd(item, attr);
            }
        }
    },

    attrRemoveByName(itemName, attr, items) {
        //console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                this.attrRemove(item, attr);
            }
        }
    },

    setValueDesc(item, key, value, items) {
        //console.info("setValueDesc, item: " + item.name + ", [" + key + "] <- " + value);

        // Себя
        item[key] = value;

        // Своих однофамильцев
        this.setValueByName(item.name, key, value, items);

        // Своих потомков
        for (let child of item.childs) {
            this.setValueDesc(child, key, value, items);
        }
    },

    setValueByName(itemName, key, value, items) {
        //console.info("setValueByName, itemName: " + itemName + ", [" + key + "] <- " + value);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                item[key] = value;
            }
        }
    },
}