
(function () {
    try {
        if (window.StorageService && typeof window.StorageService.setAdapter === 'function' && window.ApiAdapter) {
            window.StorageService.setAdapter(window.ApiAdapter);
        }
    } catch (e) {
    }
})();
