# Padrão Visual — Hub IaTechHub

Guia de referência obrigatório para todos os sistemas da plataforma IaTechHub.
Sempre consulte este arquivo antes de criar ou modificar qualquer interface.

---

## Identidade Visual

### Paleta de Cores

| Papel | Hex | Uso |
|---|---|---|
| Primária | `#00c8a0` | Botões principais, destaques, badges ativos |
| Secundária | `#4e73df` | Links, gráficos secundários, ícones de info |
| Perigo | `#e74a3b` | Erros, exclusões, alertas críticos |
| Aviso | `#f6c23e` | Warnings, pendências, atenção |
| Sidebar BG | `#1a1a2e` | Fundo da sidebar (fixo, independe do modo) |
| Topbar BG | `#16213e` | Fundo da topbar (fixo, independe do modo) |

### Modo Claro / Escuro

```css
/* Light mode */
--bg-main:    #f4f6fc;
--bg-card:    #ffffff;
--text-main:  #212529;
--text-muted: #6c757d;
--border:     #dee2e6;

/* Dark mode  (classe `dark` no <body>) */
--bg-main:    #0f0f23;
--bg-card:    #1e1e3a;
--text-main:  #e9ecef;
--text-muted: #adb5bd;
--border:     #2d2d4e;
```

---

## Tipografia e Ícones

```html
<!-- Google Fonts — Inter -->
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">

<!-- Font Awesome 6 -->
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
```

```css
body {
    font-family: 'Inter', sans-serif;
    font-size: 14px;
    line-height: 1.6;
}

h1 { font-size: 1.5rem; font-weight: 700; }
h2 { font-size: 1.25rem; font-weight: 600; }
h3 { font-size: 1rem;    font-weight: 600; }
```

---

## Layout

### Estrutura base

```
┌─────────────────────────────────────────┐
│          TOPBAR (60px fixo)             │
├──────────┬──────────────────────────────┤
│          │                              │
│ SIDEBAR  │     CONTEÚDO PRINCIPAL       │
│  220px   │     max-width: 1400px        │
│  fixo    │     padding: 1.5rem          │
│          │                              │
└──────────┴──────────────────────────────┘
```

### HTML estrutural

```html
<body>
  <div class="app-wrapper">

    <!-- Sidebar -->
    <aside class="sidebar" id="sidebar">
      <div class="sidebar-logo">
        <img src="/assets/logo.png" alt="IaTechHub">
        <span>IaTechHub</span>
      </div>
      <nav class="sidebar-nav">
        <a href="#" class="nav-item active">
          <i class="fa-solid fa-gauge"></i> Dashboard
        </a>
        <!-- ... -->
      </nav>
    </aside>

    <!-- Main -->
    <div class="main-wrapper">

      <!-- Topbar -->
      <header class="topbar">
        <button class="sidebar-toggle" id="sidebarToggle">
          <i class="fa-solid fa-bars"></i>
        </button>
        <div class="topbar-right">
          <button class="theme-toggle" id="themeToggle" title="Alternar tema">
            <i class="fa-solid fa-moon" id="themeIcon"></i>
          </button>
          <!-- user avatar, notificações, etc. -->
        </div>
      </header>

      <!-- Conteúdo -->
      <main class="content">
        <div class="page-container">
          <!-- ... -->
        </div>
      </main>

    </div><!-- /main-wrapper -->
  </div><!-- /app-wrapper -->
</body>
```

### CSS — Layout principal

```css
/* Reset */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: 'Inter', sans-serif;
    background: var(--bg-main);
    color: var(--text-main);
    transition: background 0.3s, color 0.3s;
}

/* Wrapper */
.app-wrapper {
    display: flex;
    min-height: 100vh;
}

/* Sidebar */
.sidebar {
    width: 220px;
    min-height: 100vh;
    background: #1a1a2e;
    position: fixed;
    top: 0; left: 0;
    display: flex;
    flex-direction: column;
    z-index: 1000;
    transition: transform 0.3s ease;
}

.sidebar-logo {
    height: 60px;
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0 1.25rem;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    color: #fff;
    font-weight: 700;
    font-size: 1rem;
}

.sidebar-logo img { width: 28px; height: 28px; }

/* Nav itens */
.sidebar-nav { padding: 1rem 0; flex: 1; }

.nav-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.65rem 1.25rem;
    color: rgba(255,255,255,0.65);
    text-decoration: none;
    font-size: 0.875rem;
    font-weight: 500;
    border-left: 3px solid transparent;
    transition: all 0.2s;
}

.nav-item:hover,
.nav-item.active {
    color: #fff;
    background: rgba(0,200,160,0.12);
    border-left-color: #00c8a0;
}

.nav-item i { width: 18px; text-align: center; }

/* Main wrapper */
.main-wrapper {
    margin-left: 220px;
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 100vh;
}

/* Topbar */
.topbar {
    height: 60px;
    background: #16213e;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 1.5rem;
    position: sticky;
    top: 0;
    z-index: 999;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
}

.topbar-right {
    display: flex;
    align-items: center;
    gap: 1rem;
}

/* Botão toggle tema */
.theme-toggle,
.sidebar-toggle {
    background: none;
    border: none;
    color: rgba(255,255,255,0.8);
    font-size: 1rem;
    cursor: pointer;
    padding: 0.4rem 0.6rem;
    border-radius: 6px;
    transition: background 0.2s, color 0.2s;
}

.theme-toggle:hover,
.sidebar-toggle:hover {
    background: rgba(255,255,255,0.1);
    color: #fff;
}

/* Conteúdo */
.content {
    flex: 1;
    padding: 1.5rem;
    overflow-y: auto;
}

.page-container {
    max-width: 1400px;
    margin: 0 auto;
}
```

---

## Componentes

### Cards

```css
.card {
    background: var(--bg-card);
    border-radius: 12px;
    padding: 1.5rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    border: 1px solid var(--border);
    transition: box-shadow 0.2s;
}

.card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.12); }

.card-title {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    margin-bottom: 0.5rem;
}

.card-value {
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--text-main);
}
```

```html
<!-- Card de métrica -->
<div class="card">
  <div class="card-title">
    <i class="fa-solid fa-chart-line"></i> Total de Vendas
  </div>
  <div class="card-value">R$ 48.320</div>
  <div class="card-delta positive">
    <i class="fa-solid fa-arrow-up"></i> 12,4% vs mês anterior
  </div>
</div>
```

### Botões

```css
.btn {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.5rem 1.1rem;
    border-radius: 8px;
    font-size: 0.875rem;
    font-weight: 500;
    border: none;
    cursor: pointer;
    transition: opacity 0.2s, transform 0.1s;
}

.btn:active { transform: scale(0.98); }

.btn-primary   { background: #00c8a0; color: #fff; }
.btn-secondary { background: #4e73df; color: #fff; }
.btn-danger    { background: #e74a3b; color: #fff; }
.btn-warning   { background: #f6c23e; color: #212529; }
.btn-outline   { background: transparent; border: 1px solid var(--border); color: var(--text-main); }

.btn:hover { opacity: 0.88; }
```

### Badges

```css
.badge {
    display: inline-block;
    padding: 0.2rem 0.6rem;
    border-radius: 20px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

.badge-success { background: rgba(0,200,160,0.15); color: #00c8a0; }
.badge-info    { background: rgba(78,115,223,0.15); color: #4e73df; }
.badge-danger  { background: rgba(231,74,59,0.15);  color: #e74a3b; }
.badge-warning { background: rgba(246,194,62,0.15); color: #c9a227; }
```

### Tabelas

```css
.table-wrapper {
    overflow-x: auto;
    border-radius: 12px;
    border: 1px solid var(--border);
}

table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
}

thead th {
    background: var(--bg-card);
    padding: 0.75rem 1rem;
    text-align: left;
    font-weight: 600;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    border-bottom: 2px solid var(--border);
}

tbody td {
    padding: 0.75rem 1rem;
    border-bottom: 1px solid var(--border);
    color: var(--text-main);
}

tbody tr:last-child td { border-bottom: none; }
tbody tr:hover { background: rgba(0,200,160,0.04); }
```

### Formulários

```css
.form-group { margin-bottom: 1rem; }

.form-label {
    display: block;
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--text-muted);
    margin-bottom: 0.4rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

.form-control {
    width: 100%;
    padding: 0.55rem 0.9rem;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-main);
    color: var(--text-main);
    font-family: 'Inter', sans-serif;
    font-size: 0.875rem;
    transition: border-color 0.2s, box-shadow 0.2s;
}

.form-control:focus {
    outline: none;
    border-color: #00c8a0;
    box-shadow: 0 0 0 3px rgba(0,200,160,0.15);
}
```

---

## Gráficos — Chart.js

```html
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
```

```js
// Paleta padrão IaTechHub
const CHART_COLORS = {
    primary:   '#00c8a0',
    secondary: '#4e73df',
    danger:    '#e74a3b',
    warning:   '#f6c23e',
};

// Defaults globais (aplicar uma vez no início)
Chart.defaults.font.family = 'Inter, sans-serif';
Chart.defaults.font.size   = 12;
Chart.defaults.color       = '#adb5bd';
Chart.defaults.plugins.legend.position = 'bottom';

// Exemplo — Linha
const ctx = document.getElementById('myChart').getContext('2d');
new Chart(ctx, {
    type: 'line',
    data: {
        labels: ['Jan','Fev','Mar','Abr','Mai','Jun'],
        datasets: [{
            label: 'Receita',
            data: [12000, 19000, 15000, 24000, 18000, 30000],
            borderColor: CHART_COLORS.primary,
            backgroundColor: 'rgba(0,200,160,0.1)',
            borderWidth: 2,
            fill: true,
            tension: 0.4,
            pointBackgroundColor: CHART_COLORS.primary,
        }]
    },
    options: {
        responsive: true,
        plugins: { legend: { display: true } },
        scales: {
            x: { grid: { color: 'rgba(255,255,255,0.05)' } },
            y: { grid: { color: 'rgba(255,255,255,0.05)' } },
        }
    }
});
```

---

## Dark/Light Mode

```js
// Aplicar na inicialização da página
(function () {
    const saved = localStorage.getItem('iatechhub-theme') || 'dark';
    document.body.classList.toggle('dark', saved === 'dark');
    updateThemeIcon(saved);
})();

document.getElementById('themeToggle').addEventListener('click', function () {
    const isDark = document.body.classList.toggle('dark');
    const theme  = isDark ? 'dark' : 'light';
    localStorage.setItem('iatechhub-theme', theme);
    updateThemeIcon(theme);
});

function updateThemeIcon(theme) {
    const icon = document.getElementById('themeIcon');
    icon.className = theme === 'dark'
        ? 'fa-solid fa-moon'
        : 'fa-solid fa-sun';
}
```

---

## Responsividade — Mobile

```css
@media (max-width: 768px) {

    .sidebar {
        transform: translateX(-220px);
    }

    .sidebar.open {
        transform: translateX(0);
    }

    /* Overlay escuro ao abrir sidebar */
    .sidebar-overlay {
        display: none;
        position: fixed;
        inset: 0;
        background: rgba(0,0,0,0.5);
        z-index: 999;
    }

    .sidebar-overlay.active { display: block; }

    .main-wrapper { margin-left: 0; }

    .content { padding: 1rem; }

    .page-container { max-width: 100%; }
}
```

```js
const sidebar        = document.getElementById('sidebar');
const sidebarToggle  = document.getElementById('sidebarToggle');
const overlay        = document.getElementById('sidebarOverlay');

sidebarToggle.addEventListener('click', () => {
    sidebar.classList.toggle('open');
    overlay.classList.toggle('active');
});

overlay.addEventListener('click', () => {
    sidebar.classList.remove('open');
    overlay.classList.remove('active');
});
```

---

## Requisições — fetch() nativo

```js
// GET
async function getData(endpoint) {
    const res = await fetch(`/api/${endpoint}`, {
        headers: { 'Content-Type': 'application/json' }
    });
    if (!res.ok) throw new Error(`Erro ${res.status}`);
    return res.json();
}

// POST
async function postData(endpoint, body) {
    const res = await fetch(`/api/${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`Erro ${res.status}`);
    return res.json();
}

// Uso com feedback visual
async function loadDashboard() {
    try {
        showLoading();
        const data = await getData('dashboard');
        renderMetrics(data);
    } catch (err) {
        showToast('Erro ao carregar dados', 'danger');
        console.error(err);
    } finally {
        hideLoading();
    }
}
```

---

## Regras Gerais de Desenvolvimento

- **Sem jQuery** — usar apenas JavaScript vanilla e `fetch()` nativo
- **Sem frameworks CSS** — implementar o padrão deste guia diretamente
- **Sem build step** — HTML/CSS/JS puros, sem Webpack/Vite nos projetos simples
- **Chart.js via CDN** — não instalar via npm nos projetos de painel
- **Consistência primeiro** — qualquer nova tela deve seguir este padrão antes de qualquer estilo próprio
- **localStorage** para preferências do usuário (tema, estado da sidebar)
- **Acessibilidade mínima** — `aria-label` em botões de ícone, contraste adequado
- **Mobile first** — testar sempre em viewport 375px antes de declarar concluído

---

*IaTechHub — Padrão Visual v1.0*
