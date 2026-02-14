/* Theme toggle handling aligned with StockSignal styles */
(function() {
    function applyTheme(theme) {
        const root = document.documentElement;
        const toggle = document.getElementById('themeToggle');
        if (theme === 'light') {
            root.style.setProperty('--bg-primary', '#f8fafc');
            root.style.setProperty('--bg-secondary', '#ffffff');
            root.style.setProperty('--bg-tertiary', '#e2e8f0');
            root.style.setProperty('--text-primary', '#0f172a');
            root.style.setProperty('--text-secondary', '#475569');
            root.style.setProperty('--border-color', '#e2e8f0');
            document.body.classList.add('light-theme');
            if (toggle) {
                toggle.classList.add('light-mode');
                toggle.innerHTML = '<i class="fas fa-sun"></i>';
                toggle.title = 'Switch to Dark Mode';
            }
        } else {
            root.style.setProperty('--bg-primary', '#0f172a');
            root.style.setProperty('--bg-secondary', '#1e293b');
            root.style.setProperty('--bg-tertiary', '#334155');
            root.style.setProperty('--text-primary', '#f1f5f9');
            root.style.setProperty('--text-secondary', '#cbd5e1');
            root.style.setProperty('--border-color', '#475569');
            document.body.classList.remove('light-theme');
            if (toggle) {
                toggle.classList.remove('light-mode');
                toggle.innerHTML = '<i class="fas fa-moon"></i>';
                toggle.title = 'Switch to Light Mode';
            }
        }
    }

    function toggleTheme() {
        const current = localStorage.getItem('theme') || 'dark';
        const next = current === 'dark' ? 'light' : 'dark';
        localStorage.setItem('theme', next);
        applyTheme(next);
    }

    document.addEventListener('DOMContentLoaded', function() {
        const saved = localStorage.getItem('theme') || 'dark';
        applyTheme(saved);
        const toggle = document.getElementById('themeToggle');
        if (toggle) {
            toggle.addEventListener('click', function() {
                toggleTheme();
            });
        }
    });
})();
