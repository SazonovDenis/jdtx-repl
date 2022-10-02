export {utItems}


const utItems = {
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

    attrRemoveDesc(item, attr, items) {
        // Себя
        this.attrRemove(item, attr)

        // Своих однофамильцев
        this.attrRemoveByName(item.name, attr, items);

        // Потомков
        for (let child of item.childs) {
            this.attrRemoveDesc(child, attr, items)
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

    attrAddByName(itemName, attr, items) {
        console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                this.attrAdd(item, attr);
            }
        }
    },

    attrRemoveByName(itemName, attr, items) {
        console.info("attrAddByName, attr: " + attr);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                this.attrRemove(item, attr);
            }
        }
    },

    setValueDesc(item, key, value, items) {
        console.info("setValueDesc, item: " + item.name + ", [" + key + "] <- " + value);

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
        console.info("setValueByName, itemName: " + itemName + ", [" + key + "] <- " + value);

        // Все элементы c таким именем
        for (let item of items) {
            if (item.name == itemName) {
                item[key] = value;
            }
        }
    },
}