(function () {
    const ApiAdapter = {
        getExpenses: async function () { throw new Error('ApiAdapter not implemented'); },
        saveExpenses: async function () { throw new Error('ApiAdapter not implemented'); },

        getSuppliers: async function () { throw new Error('ApiAdapter not implemented'); },
        saveSuppliers: async function () { throw new Error('ApiAdapter not implemented'); },

        getEmployees: async function () { throw new Error('ApiAdapter not implemented'); },
        saveEmployees: async function () { throw new Error('ApiAdapter not implemented'); },

        getCustomers: async function () { throw new Error('ApiAdapter not implemented'); },
        saveCustomers: async function () { throw new Error('ApiAdapter not implemented'); },

        getExpenseCategories: async function () { throw new Error('ApiAdapter not implemented'); },
        saveExpenseCategories: async function () { throw new Error('ApiAdapter not implemented'); },

        getInventoryProducts: async function () { throw new Error('ApiAdapter not implemented'); },
        saveInventoryProducts: async function () { throw new Error('ApiAdapter not implemented'); },

        getInventoryLots: async function () { throw new Error('ApiAdapter not implemented'); },
        saveInventoryLots: async function () { throw new Error('ApiAdapter not implemented'); },

        getInventoryMovements: async function () { throw new Error('ApiAdapter not implemented'); },
        saveInventoryMovements: async function () { throw new Error('ApiAdapter not implemented'); },

        getSales: async function () { throw new Error('ApiAdapter not implemented'); },
        saveSales: async function () { throw new Error('ApiAdapter not implemented'); }
    };

    window.ApiAdapter = ApiAdapter;
})();
