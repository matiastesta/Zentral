(function () {
    function _qs(params) {
        try {
            const p = params && typeof params === 'object' ? params : {};
            const q = new URLSearchParams();
            Object.keys(p).forEach(k => {
                const v = p[k];
                if (v == null) return;
                const s = String(v).trim();
                if (!s) return;
                q.set(String(k), s);
            });
            const out = q.toString();
            return out ? ('?' + out) : '';
        } catch (e) {
            return '';
        }
    }

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
        getExpenses: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/expenses/api/expenses' + _qs({
                from: o.from,
                to: o.to,
                category: o.category,
                limit: o.limit
            }));
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

        getCustomers: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/customers/api/customers' + _qs({
                q: o.q,
                limit: o.limit
            }));
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

        getInventoryProducts: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/inventory/api/products' + _qs({
                limit: o.limit || 5000,
                active: o.active
            }));
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

        getInventoryLots: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/inventory/api/lots' + _qs({
                limit: o.limit || 10000,
                product_id: o.product_id
            }));
            return Array.isArray(data.items) ? data.items : [];
        },
        saveInventoryLots: async function (arr) {
            // Inventory lots don't have a generic update endpoint; keep this as a safe no-op.
            return Array.isArray(arr) ? arr : [];
        },

        getInventoryMovements: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/inventory/api/movements' + _qs({
                from: o.from,
                to: o.to,
                product_id: o.product_id,
                limit: o.limit || 2000
            }));
            return Array.isArray(data.items) ? data.items : [];
        },
        saveInventoryMovements: async function (arr) {
            // Inventory movements are derived from operations; keep this as a safe no-op.
            return Array.isArray(arr) ? arr : [];
        },

        getSales: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/sales/api/sales' + _qs({
                from: o.from,
                to: o.to,
                include_replaced: o.include_replaced,
                exclude_cc: o.exclude_cc,
                limit: o.limit
            }));
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
        },

        getCashCounts: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/movements/api/cash-counts' + _qs({
                from: o.from,
                to: o.to
            }));
            return Array.isArray(data.items) ? data.items : [];
        },

        getOverdueCustomersCount: async function (opts) {
            const o = opts && typeof opts === 'object' ? opts : {};
            const data = await _json('/sales/api/sales/overdue-customers' + _qs({
                days: o.days
            }));
            return data && typeof data.count === 'number' ? data.count : 0;
        }
    };

    window.ApiAdapter = ApiAdapter;
})();
