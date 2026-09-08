// Frontend Logic for E-Commerce Lakehouse Analytics Platform

let revenueChartInstance = null;
let categoryChartInstance = null;
let segmentChartInstance = null;

document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initSqlPresets();
    loadDashboardMetrics();
    loadDataQuality();
    loadTableExplorer("silver_sales");
    
    // Event listeners
    document.getElementById('refresh-all-btn').addEventListener('click', () => {
        loadDashboardMetrics();
        loadDataQuality();
        logTerminal("Refreshing metrics and telemetry from OneLake...");
    });
    
    document.getElementById('trigger-pipeline-header-btn').addEventListener('click', runFullPipeline);
    const masterBtn = document.getElementById('trigger-pipeline-master-btn');
    if (masterBtn) masterBtn.addEventListener('click', runFullPipeline);
    
    document.getElementById('run-bronze-btn').addEventListener('click', () => runPipelineStage("bronze"));
    document.getElementById('run-silver-btn').addEventListener('click', () => runPipelineStage("silver"));
    document.getElementById('run-gold-btn').addEventListener('click', () => runPipelineStage("gold"));
    document.getElementById('run-sql-btn').addEventListener('click', executeCustomSql);
    document.getElementById('table-select').addEventListener('change', (e) => loadTableExplorer(e.target.value));
});

// Open Table directly in Explorer
function openExplorerTable(tableName) {
    const navBtn = document.querySelector('.nav-btn[data-tab="explorer"]');
    if (navBtn) navBtn.click();
    
    const select = document.getElementById('table-select');
    if (select) {
        select.value = tableName;
        loadTableExplorer(tableName);
    }
}

// Navigation Tabs
function initNavigation() {
    const navBtns = document.querySelectorAll('.nav-btn');
    const tabs = document.querySelectorAll('.tab-content');
    const titleMap = {
        'dashboard': { title: 'Executive Analytics Dashboard', sub: 'Real-time e-commerce operational intelligence & medallion analytics' },
        'pipeline': { title: 'Pipeline Operations Center', sub: 'Fabric Data Factory Medallion Transformation Engine' },
        'quality': { title: 'Data Quality & Governance', sub: 'Automated table-level audit statistics and quality rule evaluations' },
        'sql': { title: 'SQL Analytics Studio', sub: 'Serverless T-SQL query endpoint over Gold Lakehouse Delta tables' },
        'explorer': { title: 'Lakehouse Data Explorer', sub: 'Browse raw landing files, Silver Delta tables, and Gold Star Schema' }
    };

    navBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const target = btn.dataset.tab;
            
            navBtns.forEach(b => b.classList.remove('active'));
            tabs.forEach(t => t.classList.remove('active'));
            
            btn.classList.add('active');
            document.getElementById(`tab-${target}`).classList.add('active');
            
            if (titleMap[target]) {
                document.getElementById('page-title').textContent = titleMap[target].title;
                document.getElementById('page-subtitle').textContent = titleMap[target].sub;
            }
        });
    });
}

// SQL Presets Map
const SQL_PRESETS = {
    '01': `SELECT d.year, d.month, d.month_name, SUM(f.sales_amount) AS revenue, SUM(f.profit_amount) AS profit, COUNT(DISTINCT f.order_id) AS orders FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key GROUP BY d.year, d.month, d.month_name ORDER BY d.year, d.month;`,
    '02': `SELECT p.product_name, p.category_name, SUM(f.quantity) AS units_sold, SUM(f.sales_amount) AS revenue, SUM(f.profit_amount) AS profit FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key GROUP BY p.product_name, p.category_name ORDER BY revenue DESC LIMIT 10;`,
    '03': `SELECT c.customer_id, c.full_name, c.customer_segment, COUNT(DISTINCT f.order_id) AS total_orders, SUM(f.sales_amount) AS total_spend FROM fact_sales f JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.is_current = true GROUP BY c.customer_id, c.full_name, c.customer_segment ORDER BY total_spend DESC LIMIT 10;`,
    '04': `SELECT p.category_name, COUNT(DISTINCT r.order_id) AS total_returns, SUM(r.refund_amount) AS total_refunded FROM fact_returns r JOIN dim_product p ON r.product_key = p.product_key GROUP BY p.category_name ORDER BY total_refunded DESC;`
};

function initSqlPresets() {
    const select = document.getElementById('sql-preset-select');
    const editor = document.getElementById('sql-editor');
    
    editor.value = SQL_PRESETS['01'];
    select.addEventListener('change', (e) => {
        if (SQL_PRESETS[e.target.value]) {
            editor.value = SQL_PRESETS[e.target.value];
        }
    });
}

// Fetch Executive Dashboard Metrics
async function loadDashboardMetrics() {
    try {
        const [sumRes, trendRes, catRes, segRes] = await Promise.all([
            fetch('/api/metrics/summary'),
            fetch('/api/metrics/revenue-trend'),
            fetch('/api/metrics/categories'),
            fetch('/api/metrics/customer-segments')
        ]);
        
        const summary = await sumRes.json();
        const trend = await trendRes.json();
        const categories = await catRes.json();
        const segments = await segRes.json();
        
        // Update KPI Cards
        document.getElementById('kpi-revenue').textContent = `$${(summary.total_revenue || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        document.getElementById('kpi-orders').textContent = (summary.total_orders || 0).toLocaleString('en-US');
        document.getElementById('kpi-aov').textContent = `$${(summary.average_order_value || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        document.getElementById('kpi-profit').textContent = `$${(summary.total_profit || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        document.getElementById('kpi-margin').textContent = `${summary.profit_margin_pct || 0}%`;

        // Render Charts
        renderRevenueTrendChart(trend);
        renderCategoryChart(categories);
        renderSegmentChart(segments);
        
    } catch (err) {
        console.error("Error loading dashboard metrics:", err);
    }
}

// Render Revenue & Profit Chart
function renderRevenueTrendChart(data) {
    const ctx = document.getElementById('revenueTrendChart').getContext('2d');
    const labels = data.map(d => `${d.month_name} ${d.year}`);
    const revenues = data.map(d => d.revenue);
    const profits = data.map(d => d.profit);
    
    if (revenueChartInstance) revenueChartInstance.destroy();
    
    revenueChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Gross Revenue ($)',
                    data: revenues,
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99, 102, 241, 0.1)',
                    fill: true,
                    tension: 0.3
                },
                {
                    label: 'Net Profit ($)',
                    data: profits,
                    borderColor: '#a855f7',
                    backgroundColor: 'rgba(168, 85, 247, 0.1)',
                    fill: true,
                    tension: 0.3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { labels: { color: '#94a3b8' } } },
            scales: {
                x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } }
            }
        }
    });
}

// Render Category Chart
function renderCategoryChart(data) {
    const ctx = document.getElementById('categoryChart').getContext('2d');
    const labels = data.map(d => d.category_name);
    const revenues = data.map(d => d.category_revenue);
    
    if (categoryChartInstance) categoryChartInstance.destroy();
    
    categoryChartInstance = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: revenues,
                backgroundColor: ['#6366f1', '#06b6d4', '#10b981', '#a855f7', '#f43f5e']
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8' } } }
        }
    });
}

// Render Segment Chart
function renderSegmentChart(data) {
    const ctx = document.getElementById('segmentChart').getContext('2d');
    const labels = data.map(d => d.customer_segment);
    const spend = data.map(d => d.segment_revenue);
    
    if (segmentChartInstance) segmentChartInstance.destroy();
    
    segmentChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Revenue ($)',
                data: spend,
                backgroundColor: '#06b6d4',
                borderRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { color: '#94a3b8' }, grid: { display: false } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } }
            }
        }
    });
}

// Global Data Quality Cache
let rawDqData = [];
let activeGovCategory = 'ALL';

// Load Data Quality Results & Update Governance KPI Cards
async function loadDataQuality() {
    try {
        const res = await fetch('/api/metrics/quality');
        rawDqData = await res.json();
        
        // Calculate KPI Metrics
        const totalChecks = rawDqData.length;
        const passedChecks = rawDqData.filter(d => d.status === 'PASSED').length;
        const criticalTotal = rawDqData.filter(d => d.severity === 'CRITICAL').length;
        const criticalPassed = rawDqData.filter(d => d.severity === 'CRITICAL' && d.status === 'PASSED').length;
        const totalFailedRecords = rawDqData.reduce((acc, curr) => acc + (curr.failed_records || 0), 0);
        
        const overallScore = totalChecks > 0 ? ((passedChecks / totalChecks) * 100).toFixed(1) : "100.0";
        
        const scoreElem = document.getElementById('dq-kpi-score');
        if (scoreElem) scoreElem.textContent = `${overallScore}%`;
        
        const critElem = document.getElementById('dq-kpi-critical');
        if (critElem) critElem.textContent = `${criticalPassed} / ${criticalTotal}`;
        
        const checkElem = document.getElementById('dq-kpi-total-checks');
        if (checkElem) checkElem.textContent = `${totalChecks} Rules`;
        
        const anomElem = document.getElementById('dq-kpi-anomalies');
        if (anomElem) anomElem.textContent = `${totalFailedRecords} Records`;

        renderDqTable();
        loadDataCatalog();
    } catch (err) {
        console.error("Error loading quality data:", err);
    }
}

// Render Data Quality Audit Table based on Active Category Filter
function renderDqTable() {
    const tbody = document.getElementById('quality-table-body');
    if (!tbody) return;
    tbody.innerHTML = '';
    
    const filtered = activeGovCategory === 'ALL' 
        ? rawDqData 
        : rawDqData.filter(item => (item.category || '').toUpperCase() === activeGovCategory);
    
    if (filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align:center; color:#64748b;">No governance checks found for category ${activeGovCategory}</td></tr>`;
        return;
    }

    filtered.forEach(item => {
        const tr = document.createElement('tr');
        
        let sevClass = 'sev-low';
        if (item.severity === 'CRITICAL') sevClass = 'sev-critical';
        else if (item.severity === 'HIGH') sevClass = 'sev-high';
        else if (item.severity === 'MEDIUM') sevClass = 'sev-medium';

        tr.innerHTML = `
            <td><code>${item.table_name}</code></td>
            <td><strong>${item.check_name}</strong></td>
            <td><span class="category-tag">${item.category || 'GENERAL'}</span></td>
            <td><span class="severity-badge ${sevClass}">${item.severity || 'HIGH'}</span></td>
            <td>${(item.total_records || 0).toLocaleString()}</td>
            <td><strong style="color: ${item.failed_records > 0 ? '#f43f5e' : '#10b981'}">${(item.failed_records || 0).toLocaleString()}</strong></td>
            <td>
                <div class="progress-bar-wrap">
                    <div class="progress-bar-fill" style="width: ${item.success_percentage}%;"></div>
                    <span>${item.success_percentage}%</span>
                </div>
            </td>
            <td><span class="badge ${item.status.toLowerCase()}">${item.status}</span></td>
        `;
        tbody.appendChild(tr);
    });
}

// Category Filter Click Handler
function filterDqCategory(category) {
    activeGovCategory = category;
    const pills = document.querySelectorAll('.category-pill');
    pills.forEach(p => {
        if (p.textContent.trim() === category) p.classList.add('active');
        else p.classList.remove('active');
    });
    renderDqTable();
}

// Switch between Audit Trail and Data Catalog Subtabs
function switchGovSubtab(tabName) {
    const auditView = document.getElementById('gov-view-audit');
    const catalogView = document.getElementById('gov-view-catalog');
    const auditBtn = document.getElementById('gov-subtab-audit');
    const catalogBtn = document.getElementById('gov-subtab-catalog');
    const catPills = document.getElementById('category-pills');

    if (tabName === 'audit') {
        auditView.style.display = 'block';
        catalogView.style.display = 'none';
        auditBtn.classList.add('active');
        catalogBtn.classList.remove('active');
        catPills.style.display = 'flex';
    } else {
        auditView.style.display = 'none';
        catalogView.style.display = 'block';
        auditBtn.classList.remove('active');
        catalogBtn.classList.add('active');
        catPills.style.display = 'none';
    }
}

// Fetch and Render Data Catalog Metadata
async function loadDataCatalog() {
    try {
        const res = await fetch('/api/governance/catalog');
        const catalog = await res.json();
        
        const tbody = document.getElementById('catalog-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        
        catalog.forEach(item => {
            const tr = document.createElement('tr');
            
            let piiClass = 'pii-public';
            if (item.pii_classification.includes('CONFIDENTIAL')) piiClass = 'pii-confidential';
            else if (item.pii_classification.includes('RESTRICTED')) piiClass = 'pii-restricted';
            
            tr.innerHTML = `
                <td><code>${item.table_name}</code></td>
                <td><span class="layer-tag ${item.layer.toLowerCase()}">${item.layer}</span></td>
                <td><strong>${item.column_name}</strong></td>
                <td><code>${item.data_type}</code></td>
                <td><span class="pii-badge ${piiClass}"><i class="fa-solid fa-lock"></i> ${item.pii_classification}</span></td>
                <td><small>${item.quality_rules_applied}</small></td>
                <td>${item.retention_days} Days</td>
                <td>${item.owner}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (err) {
        console.error("Error loading data catalog:", err);
    }
}

// Load Table Explorer
async function loadTableExplorer(tableName) {
    try {
        const res = await fetch(`/api/tables/data?name=${tableName}&limit=30`);
        const result = await res.json();
        
        const thead = document.getElementById('explorer-head');
        const tbody = document.getElementById('explorer-body');
        
        document.getElementById('explorer-row-count').textContent = `Showing preview of ${result.rows.length} rows`;
        
        thead.innerHTML = '<tr>' + result.columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
        tbody.innerHTML = result.rows.map(row => 
            '<tr>' + row.map(val => `<td>${val !== null ? val : '<em style="color:#64748b">null</em>'}</td>`).join('') + '</tr>'
        ).join('');
    } catch (err) {
        console.error("Error loading explorer table:", err);
    }
}

// Execute Custom SQL Query
async function executeCustomSql() {
    const sql = document.getElementById('sql-editor').value;
    try {
        const res = await fetch('/api/sql/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sql })
        });
        
        const result = await res.json();
        
        const thead = document.getElementById('sql-results-head');
        const tbody = document.getElementById('sql-results-body');
        
        if (result.error) {
            thead.innerHTML = '<tr><th>Error</th></tr>';
            tbody.innerHTML = `<tr><td style="color:#f43f5e">${result.error}</td></tr>`;
            document.getElementById('sql-row-count').textContent = 'Error';
            return;
        }
        
        document.getElementById('sql-row-count').textContent = `${result.rows.length} rows returned`;
        
        thead.innerHTML = '<tr>' + result.columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
        tbody.innerHTML = result.rows.map(row => 
            '<tr>' + row.map(val => `<td>${val !== null ? val : '<em style="color:#64748b">null</em>'}</td>`).join('') + '</tr>'
        ).join('');
    } catch (err) {
        console.error("Error executing custom SQL:", err);
    }
}

// Run Pipeline Stage Trigger
async function runPipelineStage(stage) {
    logTerminal(`================================================================================`, 'info');
    logTerminal(`[JOB TRIGGERED] Executing Medallion Stage: ${stage.toUpperCase()}...`, 'info');
    updateLayerCardStatus(stage, '<i class="fa-solid fa-spinner fa-spin"></i> RUNNING...');
    
    try {
        const res = await fetch('/api/pipeline/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stage: stage })
        });
        
        const data = await res.json();
        
        if (data.logs && Array.isArray(data.logs)) {
            data.logs.forEach(line => {
                let type = 'info';
                if (line.includes('->')) type = 'success';
                else if (line.includes('completed successfully')) type = 'success';
                logTerminal(line, type);
            });
        }
        
        logTerminal(`[SUCCESS] Medallion Stage ${stage.toUpperCase()} completed successfully.`, 'success');
        logTerminal(`================================================================================`, 'info');
        
        updateLayerCardStatus(stage, `✓ ${stage.toUpperCase()} COMPLETED`);
        loadDashboardMetrics();
        loadDataQuality();
    } catch (err) {
        logTerminal(`[ERROR] Stage ${stage.toUpperCase()} failed: ${err.message}`, 'error');
        updateLayerCardStatus(stage, 'FAILED');
    }
}

async function runFullPipeline() {
    logTerminal('================================================================================', 'info');
    logTerminal('[JOB TRIGGERED] Starting Full Fabric Data Factory Medallion Pipeline (Bronze → Silver → Gold)...', 'info');
    await runPipelineStage('all');
}

function updateLayerCardStatus(stage, statusText) {
    const cardMap = {
        'bronze': document.getElementById('badge-bronze'),
        'silver': document.getElementById('badge-silver'),
        'gold': document.getElementById('badge-gold')
    };
    
    if (stage === 'all') {
        Object.values(cardMap).forEach(badge => {
            if (badge) badge.innerHTML = statusText;
        });
    } else if (cardMap[stage]) {
        cardMap[stage].innerHTML = statusText;
    }
}

function logTerminal(message, type = '') {
    const container = document.getElementById('terminal-logs');
    if (!container) return;
    const div = document.createElement('div');
    div.className = `log-line ${type}`;
    div.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}
