(function () {
    function _nsPrefix() {
        try {
            const ns = String(window.__storage_ns || '').trim();
            if (!ns) return '';
            return 'ns:' + ns + ':';
        } catch (e) {
            return '';
        }
    }

    function nsKey(key) {
        return _nsPrefix() + String(key || '');
    }

    function readJson(key) {
        try {
            const raw = window.localStorage.getItem(nsKey(key));
            const parsed = raw ? JSON.parse(raw) : [];
            return Array.isArray(parsed) ? parsed : [];
        } catch (e) {
            return [];
        }
    }

    function writeJson(key, value) {
        try {
            window.localStorage.setItem(nsKey(key), JSON.stringify(value));
        } catch (e) {
        }
    }

    const KEY = {
        expenses: 'egresos_dummy',
        expenseCategories: 'expense_categories_dummy',
        suppliers: 'suppliers_dummy',
        employees: 'empleados_dummy',
        customers: 'customers_dummy',
        inventoryProducts: 'inventory_products_dummy',
        inventoryLots: 'inventory_lots_dummy',
        inventoryMovements: 'inventory_movements_dummy',
        sales: 'sales_dummy'
    };

    const LocalStorageAdapter = {
        getExpenses: function () { return readJson(KEY.expenses); },
        saveExpenses: function (arr) { writeJson(KEY.expenses, Array.isArray(arr) ? arr : []); },

        getSuppliers: function () { return readJson(KEY.suppliers); },
        saveSuppliers: function (arr) { writeJson(KEY.suppliers, Array.isArray(arr) ? arr : []); },

        getEmployees: function () { return readJson(KEY.employees); },
        saveEmployees: function (arr) { writeJson(KEY.employees, Array.isArray(arr) ? arr : []); },

        getCustomers: function () { return readJson(KEY.customers); },
        saveCustomers: function (arr) { writeJson(KEY.customers, Array.isArray(arr) ? arr : []); },

        getExpenseCategories: function () { return readJson(KEY.expenseCategories); },
        saveExpenseCategories: function (arr) { writeJson(KEY.expenseCategories, Array.isArray(arr) ? arr : []); },

        getInventoryProducts: function () { return readJson(KEY.inventoryProducts); },
        saveInventoryProducts: function (arr) { writeJson(KEY.inventoryProducts, Array.isArray(arr) ? arr : []); },

        getInventoryLots: function () { return readJson(KEY.inventoryLots); },
        saveInventoryLots: function (arr) { writeJson(KEY.inventoryLots, Array.isArray(arr) ? arr : []); },

        getInventoryMovements: function () { return readJson(KEY.inventoryMovements); },
        saveInventoryMovements: function (arr) { writeJson(KEY.inventoryMovements, Array.isArray(arr) ? arr : []); },

        getSales: function () { return readJson(KEY.sales); },
        saveSales: function (arr) { writeJson(KEY.sales, Array.isArray(arr) ? arr : []); }
    };

    window.LocalStorageAdapter = LocalStorageAdapter;
})();
