(function () {
    async function _json(url, options) {
        const res = await fetch(url, {
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json'
            },
            ...(options || {})
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || (data && data.ok === false)) {
            const msg = (data && (data.message || data.error)) ? (data.message || data.error) : ('HTTP ' + res.status);
            throw new Error(msg);
        }
        return data;
    }

    const ApiAdapter = {
        getExpenses: async function () {
            const data = await _json('/expenses/api/expenses');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveExpenses: async function (arr) {
            const payload = Array.isArray(arr) ? arr : [];
            const data = await _json('/expenses/api/expenses/bulk', {
                method: 'POST',
                body: JSON.stringify({ items: payload })
            });
            return Array.isArray(data.items) ? data.items : [];
        },

        getSuppliers: async function () {
            const data = await _json('/suppliers/api/suppliers');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveSuppliers: async function (arr) {
            const payload = Array.isArray(arr) ? arr : [];
            const data = await _json('/suppliers/api/suppliers/bulk', {
                method: 'POST',
                body: JSON.stringify({ items: payload })
            });
            return Array.isArray(data.items) ? data.items : [];
        },

        getEmployees: async function () {
            const data = await _json('/employees/api/employees');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveEmployees: async function (arr) {
            const payload = Array.isArray(arr) ? arr : [];
            const data = await _json('/employees/api/employees/bulk', {
                method: 'POST',
                body: JSON.stringify({ items: payload })
            });
            return Array.isArray(data.items) ? data.items : [];
        },

        getCustomers: async function () {
            const data = await _json('/customers/api/customers');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveCustomers: async function (arr) {
            const payload = Array.isArray(arr) ? arr : [];
            const data = await _json('/customers/api/customers/bulk', {
                method: 'POST',
                body: JSON.stringify({ items: payload })
            });
            return Array.isArray(data.items) ? data.items : [];
        },

        getExpenseCategories: async function () {
            const data = await _json('/expenses/api/categories');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveExpenseCategories: async function (arr) {
            const payload = Array.isArray(arr) ? arr : [];
            const data = await _json('/expenses/api/categories/bulk', {
                method: 'POST',
                body: JSON.stringify({ items: payload })
            });
            return Array.isArray(data.items) ? data.items : [];
        },

        getInventoryProducts: async function () {
            const data = await _json('/inventory/api/products?limit=5000');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveInventoryProducts: async function (arr) {
            const list = Array.isArray(arr) ? arr : [];
            const out = [];
            for (const it of list) {
                const p = (it && typeof it === 'object') ? it : {};
                const id = p.id;
                if (id != null && String(id).trim() !== '') {
                    try {
                        const updated = await _json('/inventory/api/products/' + encodeURIComponent(String(id)), {
                            method: 'PUT',
                            body: JSON.stringify(p)
                        });
                        if (updated && updated.item) out.push(updated.item);
                        else out.push(p);
                    } catch (e) {
                        out.push(p);
                    }
                } else {
                    try {
                        const created = await _json('/inventory/api/products', {
                            method: 'POST',
                            body: JSON.stringify(p)
                        });
                        if (created && created.item) out.push(created.item);
                        else out.push(p);
                    } catch (e) {
                        out.push(p);
                    }
                }
            }
            return out;
        },

        getInventoryLots: async function () {
            const data = await _json('/inventory/api/lots?limit=10000');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveInventoryLots: async function (arr) {
            // Inventory lots don't have a generic update endpoint; keep this as a safe no-op.
            return Array.isArray(arr) ? arr : [];
        },

        getInventoryMovements: async function () {
            const data = await _json('/inventory/api/movements?limit=2000');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveInventoryMovements: async function (arr) {
            // Inventory movements are derived from operations; keep this as a safe no-op.
            return Array.isArray(arr) ? arr : [];
        },

        getSales: async function () {
            const data = await _json('/sales/api/sales');
            return Array.isArray(data.items) ? data.items : [];
        },
        saveSales: async function (arr) {
            const list = Array.isArray(arr) ? arr : [];
            const out = [];
            for (const it of list) {
                const s = (it && typeof it === 'object') ? it : {};
                const ticket = s.ticket;
                if (ticket != null && String(ticket).trim() !== '') {
                    try {
                        const updated = await _json('/sales/api/sales/' + encodeURIComponent(String(ticket)), {
                            method: 'PUT',
                            body: JSON.stringify(s)
                        });
                        if (updated && updated.item) out.push(updated.item);
                        else out.push(s);
                    } catch (e) {
                        out.push(s);
                    }
                } else {
                    try {
                        const created = await _json('/sales/api/sales', {
                            method: 'POST',
                            body: JSON.stringify(s)
                        });
                        if (created && created.item) out.push(created.item);
                        else out.push(s);
                    } catch (e) {
                        out.push(s);
                    }
                }
            }
            return out;
        }
    };

    window.ApiAdapter = ApiAdapter;
})();
