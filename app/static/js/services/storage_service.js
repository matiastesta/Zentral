(function () {
    function nowTs() {
        return Date.now();
    }

    function safeStr(v) {
        return String(v ?? '');
    }

    function normalizeExpense(exp) {
        const e = exp && typeof exp === 'object' ? exp : {};
        const origin = (e.origin === 'inventory' || e.origin === 'manual')
            ? e.origin
            : (e.origin === 'Inventory' || e.origin === 'Desde Inventario' || e.origin === 'InventoryOrigin')
                ? 'inventory'
                : (e.origin ? safeStr(e.origin) : 'manual');

        const originNorm = (origin === 'inventory') ? 'inventory' : 'manual';
        const createdAt = typeof e.created_at === 'number' ? e.created_at : nowTs();
        const originRef = (e.origin_ref && typeof e.origin_ref === 'object') ? e.origin_ref : { product_id: null, lot_id: null, inventory_movement_id: null };

        return {
            ...e,
            origin: originNorm,
            origin_ref: {
                product_id: originRef.product_id ?? null,
                lot_id: originRef.lot_id ?? null,
                inventory_movement_id: originRef.inventory_movement_id ?? null
            },
            created_at: createdAt
        };
    }

    function ensureArray(x) {
        return Array.isArray(x) ? x : [];
    }

    const StorageService = {
        _adapter: null,

        setAdapter: function (adapter) {
            this._adapter = adapter;
        },

        getAdapter: function () {
            if (this._adapter) return this._adapter;
            if (window.ApiAdapter) {
                this._adapter = window.ApiAdapter;
                return this._adapter;
            }
            throw new Error('No storage adapter available');
        },

        getExpenses: function (opts) {
            const a = this.getAdapter();
            const res = a.getExpenses(opts);
            if (res && typeof res.then === 'function') return res.then(arr => ensureArray(arr).map(normalizeExpense));
            return ensureArray(res).map(normalizeExpense);
        },

        saveExpenses: function (arr) {
            const a = this.getAdapter();
            const payload = ensureArray(arr).map(normalizeExpense);
            return a.saveExpenses(payload);
        },

        upsertExpense: function (expense) {
            const next = normalizeExpense(expense);
            const maybe = this.getExpenses();
            if (maybe && typeof maybe.then === 'function') {
                return maybe.then(list => {
                    const arr = ensureArray(list);
                    const id = safeStr(next.id || nowTs());
                    next.id = id;
                    const idx = arr.findIndex(x => safeStr(x?.id) === id);
                    if (idx >= 0) arr[idx] = next;
                    else arr.push(next);
                    return this.saveExpenses(arr);
                });
            }
            const arr = ensureArray(maybe);
            const id = safeStr(next.id || nowTs());
            next.id = id;
            const idx = arr.findIndex(x => safeStr(x?.id) === id);
            if (idx >= 0) arr[idx] = next;
            else arr.push(next);
            return this.saveExpenses(arr);
        },

        addExpenseFromInventory: function (expense) {
            const e = normalizeExpense({ ...expense, origin: 'inventory' });
            return this.upsertExpense(e);
        },

        getInventoryProducts: function (opts) {
            const a = this.getAdapter();
            const res = a.getInventoryProducts(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveInventoryProducts: function (arr) {
            const a = this.getAdapter();
            return a.saveInventoryProducts(ensureArray(arr));
        },

        addInventoryProduct: function (product) {
            const p = product && typeof product === 'object' ? product : {};
            const id = safeStr(p.id || nowTs());
            const payload = { ...p, id };
            const maybe = this.getInventoryProducts();
            if (maybe && typeof maybe.then === 'function') {
                return maybe.then(list => {
                    const arr = ensureArray(list);
                    arr.push(payload);
                    return this.saveInventoryProducts(arr);
                });
            }
            const arr = ensureArray(maybe);
            arr.push(payload);
            return this.saveInventoryProducts(arr);
        },

        getInventoryLots: function (opts) {
            const a = this.getAdapter();
            const res = a.getInventoryLots(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveInventoryLots: function (arr) {
            const a = this.getAdapter();
            return a.saveInventoryLots(ensureArray(arr));
        },

        getInventoryMovements: function (opts) {
            const a = this.getAdapter();
            if (!a || typeof a.getInventoryMovements !== 'function') return [];
            const res = a.getInventoryMovements(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveInventoryMovements: function (arr) {
            const a = this.getAdapter();
            if (!a || typeof a.saveInventoryMovements !== 'function') return;
            return a.saveInventoryMovements(ensureArray(arr));
        },

        addInventoryMovement: function (movement) {
            const m = movement && typeof movement === 'object' ? movement : {};
            const id = safeStr(m.id || nowTs());
            const payload = { ...m, id };
            const maybe = this.getInventoryMovements();
            if (maybe && typeof maybe.then === 'function') {
                return maybe.then(list => {
                    const arr = ensureArray(list);
                    arr.push(payload);
                    return this.saveInventoryMovements(arr);
                });
            }
            const arr = ensureArray(maybe);
            arr.push(payload);
            return this.saveInventoryMovements(arr);
        },

        addInventoryLot: function (lot, expensePayload) {
            const l = lot && typeof lot === 'object' ? lot : {};
            const lotId = safeStr(l.id || nowTs());
            const payload = { ...l, id: lotId };
            const maybe = this.getInventoryLots();
            const finalize = (arr) => {
                arr.push(payload);
                const saveLotsRes = this.saveInventoryLots(arr);
                const exp = expensePayload && typeof expensePayload === 'object' ? expensePayload : null;
                if (!exp) return saveLotsRes;
                return this.addExpenseFromInventory({
                    ...exp,
                    origin_ref: { product_id: payload.product_id ?? null, lot_id: payload.id, inventory_movement_id: null }
                });
            };
            if (maybe && typeof maybe.then === 'function') {
                return maybe.then(list => finalize(ensureArray(list)));
            }
            return finalize(ensureArray(maybe));
        },

        getEmployees: function () {
            const a = this.getAdapter();
            if (!a || typeof a.getEmployees !== 'function') return [];
            const res = a.getEmployees();
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveEmployees: function (arr) {
            const a = this.getAdapter();
            if (!a || typeof a.saveEmployees !== 'function') return;
            return a.saveEmployees(ensureArray(arr));
        },

        getSuppliers: function (opts) {
            const a = this.getAdapter();
            const res = a.getSuppliers(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveSuppliers: function (arr) {
            const a = this.getAdapter();
            return a.saveSuppliers(ensureArray(arr));
        },

        getCustomers: function (opts) {
            const a = this.getAdapter();
            const res = a.getCustomers(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        saveCustomers: function (arr) {
            const a = this.getAdapter();
            return a.saveCustomers(ensureArray(arr));
        },

        getSales: function (opts) {
            const a = this.getAdapter();
            const res = a.getSales(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        getCashCounts: function (opts) {
            const a = this.getAdapter();
            if (!a || typeof a.getCashCounts !== 'function') return [];
            const res = a.getCashCounts(opts);
            return (res && typeof res.then === 'function') ? res.then(ensureArray) : ensureArray(res);
        },

        getOverdueCustomersCount: function (opts) {
            const a = this.getAdapter();
            if (!a || typeof a.getOverdueCustomersCount !== 'function') return 0;
            const res = a.getOverdueCustomersCount(opts);
            return (res && typeof res.then === 'function') ? res.then(x => (typeof x === 'number' ? x : 0)) : (typeof res === 'number' ? res : 0);
        },

        saveSales: function (arr) {
            const a = this.getAdapter();
            return a.saveSales(ensureArray(arr));
        }
    };

    window.StorageService = StorageService;
})();
