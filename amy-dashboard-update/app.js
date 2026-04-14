/**
 * Tree Permit Lead Discovery Dashboard
 * Grand Edition — Overview, Lead List, Map View, System Health
 * Connects to Supabase for real-time data, falls back to demo data.
 */

// ── Configuration ──────────────────────────────────────────────────────
// ⚠️  DEPLOYMENT: Update these 3 values for each new deployment:
//   SUPABASE_URL  — Project URL from Supabase → Settings → API
//   SUPABASE_KEY  — anon/public key from Supabase → Settings → API
//   WORKER_URL    — Cloudflare Worker URL after running: npx wrangler deploy
//                   Also set DASHBOARD_URL GitHub variable to:
//                   https://<github-username>.github.io/<dashboard-repo>/
const CONFIG = {
    SUPABASE_URL: '{{SUPABASE_URL}}',
    SUPABASE_KEY: '{{SUPABASE_KEY}}',
    WORKER_URL: '{{WORKER_URL}}',
};

// ── Supabase Client ────────────────────────────────────────────────────
let supabaseClient = null;
try {
    if (window.supabase && !CONFIG.SUPABASE_URL.includes('your-project')) {
        supabaseClient = window.supabase.createClient(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_KEY);
    }
} catch (e) {
    console.warn('Supabase not configured — using demo data.', e);
}

// ── State ──────────────────────────────────────────────────────────────
let leadGridApi = null;
let healthGridApi = null;
let historicalGridApi = null;
let allLeads = [];        // ALL leads from DB (no date filter) — used ONLY by Historical tab
let recentLeads = [];     // Last 90 days by permit_date — used by Overview, Lead List, Map
let allJobRuns = [];
let currentDetailLead = null;
let leafletMap = null;
let mapMarkers = [];
let chartTimeline = null;
let chartFreshness = null;
let chartScores = null;
let globalSearchTerm = '';
let historicalSourceFilter = '';  // '' = all, else source_name value

// ── Scoring Rules (loaded from DB, fallback to defaults) ───────────────
const SCORING_DEFAULTS = {
    tree_removal_bonus: 5,
    vegetation_removal_bonus: 4,
    landscape_installation_bonus: 3,
    recency_bonus: 3,
    recency_days_threshold: 7,
    recency_tier1_days_max: 30,
    recency_tier1_bonus: 3,
    recency_tier2_days_max: 90,
    recency_tier2_bonus: 1,
    recency_tier3_days_max: 180,
    recency_tier3_bonus: 1,
    large_parcel_bonus: 2,
    parcel_acres_threshold: 0.50,
    right_of_way_bonus: 1,
    derm_tier1_days_min: 1,
    derm_tier1_days_max: 10,
    derm_tier1_bonus: 1,
    derm_tier2_days_min: 11,
    derm_tier2_days_max: 30,
    derm_tier2_bonus: 2,
    derm_tier3_days_min: 31,
    derm_tier3_days_max: 60,
    derm_tier3_bonus: 1,
    contractor_penalty: 2,
    derm_no_address_penalty: 3,
    intended_decision_penalty: 2,
    corrections_required_penalty: 2,
    staleness_penalty: 1,
    staleness_days_threshold: 365,
};
// Rule metadata: [min, max, step, isDecimal]
const SCORING_META = {
    tree_removal_bonus:              [0, 10, 1, false],
    vegetation_removal_bonus:        [0, 10, 1, false],
    landscape_installation_bonus:    [0, 10, 1, false],
    recency_bonus:                   [0, 10, 1, false],
    recency_days_threshold:          [1, 90, 1, false],
    recency_tier1_days_max:          [1, 365, 1, false],
    recency_tier1_bonus:             [0, 10, 1, false],
    recency_tier2_days_max:          [1, 365, 1, false],
    recency_tier2_bonus:             [0, 10, 1, false],
    recency_tier3_days_max:          [1, 365, 1, false],
    recency_tier3_bonus:             [0, 10, 1, false],
    large_parcel_bonus:              [0, 10, 1, false],
    parcel_acres_threshold:          [0.10, 50.00, 0.10, true],
    right_of_way_bonus:              [0, 10, 1, false],
    derm_tier1_days_min:             [0, 365, 1, false],
    derm_tier1_days_max:             [1, 365, 1, false],
    derm_tier1_bonus:                [0, 10, 1, false],
    derm_tier2_days_min:             [0, 365, 1, false],
    derm_tier2_days_max:             [1, 365, 1, false],
    derm_tier2_bonus:                [0, 10, 1, false],
    derm_tier3_days_min:             [0, 365, 1, false],
    derm_tier3_days_max:             [1, 365, 1, false],
    derm_tier3_bonus:                [0, 10, 1, false],
    contractor_penalty:              [0, 10, 1, false],
    derm_no_address_penalty:         [0, 10, 1, false],
    intended_decision_penalty:       [0, 10, 1, false],
    corrections_required_penalty:    [0, 10, 1, false],
    staleness_penalty:               [0, 10, 1, false],
    staleness_days_threshold:        [1, 730, 1, false],
};
let scoringRules = { ...SCORING_DEFAULTS };
let scoringRulesDirty = false;

// ── Authentication State ───────────────────────────────────────────────
let isAuthenticated = false;

// Client-side fallback auth (SHA-256 hash of "username:password").
// Used when Cloudflare Worker is unreachable (Bot Fight Mode, network errors, etc.)
const _AUTH_HASH = '230e2de24881984d4e6ec5c7a0c08297960db04ba04735c34f1a8cd4657213ff';

/** Compute SHA-256 hex digest of a string (async, uses Web Crypto API) */
async function sha256(message) {
    const msgBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

/** Toggle password field visibility */
function togglePasswordVisibility() {
    const field = document.getElementById('loginPassword');
    const showIcon = document.getElementById('eyeIconShow');
    const hideIcon = document.getElementById('eyeIconHide');
    const btn = document.getElementById('togglePasswordBtn');
    if (!field) return;
    if (field.type === 'password') {
        field.type = 'text';
        if (showIcon) showIcon.classList.add('hidden');
        if (hideIcon) hideIcon.classList.remove('hidden');
        if (btn) btn.title = 'Hide password';
    } else {
        field.type = 'password';
        if (showIcon) showIcon.classList.remove('hidden');
        if (hideIcon) hideIcon.classList.add('hidden');
        if (btn) btn.title = 'Show password';
    }
}

/** Check if user has a valid session on page load. */
function initAuth() {
    const session = sessionStorage.getItem('authSession');
    if (session) {
        try {
            const parsed = JSON.parse(session);
            // Session expires after 8 hours
            if (parsed.ts && (Date.now() - parsed.ts) < 8 * 60 * 60 * 1000) {
                isAuthenticated = true;
            } else {
                sessionStorage.removeItem('authSession');
            }
        } catch { sessionStorage.removeItem('authSession'); }
    }
    applyAuthState();
}

/** Apply visual state based on auth: blur/unblur, show/hide overlay, update button */
function applyAuthState() {
    const body = document.body;
    const overlay = document.getElementById('authLockOverlay');
    const authBtn = document.getElementById('authBtn');
    const authIcon = document.getElementById('authIcon');

    if (isAuthenticated) {
        body.classList.remove('auth-locked');
        if (overlay) overlay.classList.add('hidden');
        if (authBtn) {
            authBtn.classList.add('auth-logged-in');
            authBtn.title = 'Logged in — click to log out';
        }
        if (authIcon) {
            // Checkmark icon when logged in
            authIcon.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>';
        }
    } else {
        body.classList.add('auth-locked');
        if (overlay) overlay.classList.remove('hidden');
        if (authBtn) {
            authBtn.classList.remove('auth-logged-in');
            authBtn.title = 'Login';
        }
        if (authIcon) {
            // Person icon when logged out
            authIcon.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>';
        }
    }
}

/** Guard: returns true if authenticated, otherwise shows login prompt */
function requireAuth(actionName) {
    if (isAuthenticated) return true;
    showToast('Please log in to ' + (actionName || 'use this feature'), 'error');
    openLoginModal();
    return false;
}

function handleAuthBtnClick() {
    if (isAuthenticated) {
        doLogout();
    } else {
        openLoginModal();
    }
}

function openLoginModal() {
    const modal = document.getElementById('loginModal');
    if (modal) {
        modal.classList.remove('hidden');
        const usernameField = document.getElementById('loginUsername');
        if (usernameField) setTimeout(() => usernameField.focus(), 100);
    }
    // Clear previous errors
    const err = document.getElementById('loginError');
    if (err) err.classList.add('hidden');
}

function closeLoginModal() {
    const modal = document.getElementById('loginModal');
    if (modal) modal.classList.add('hidden');
    // Clear fields and reset password visibility
    const u = document.getElementById('loginUsername');
    const p = document.getElementById('loginPassword');
    if (u) u.value = '';
    if (p) { p.value = ''; p.type = 'password'; }
    const showIcon = document.getElementById('eyeIconShow');
    const hideIcon = document.getElementById('eyeIconHide');
    if (showIcon) showIcon.classList.remove('hidden');
    if (hideIcon) hideIcon.classList.add('hidden');
    const err = document.getElementById('loginError');
    if (err) err.classList.add('hidden');
}

async function doLogin() {
    const username = (document.getElementById('loginUsername').value || '').trim();
    const password = (document.getElementById('loginPassword').value || '').trim();
    const errEl = document.getElementById('loginError');
    const submitBtn = document.getElementById('loginSubmitBtn');

    if (!username || !password) {
        if (errEl) { errEl.textContent = 'Please enter both username and password.'; errEl.classList.remove('hidden'); }
        return;
    }

    // Disable button during request
    if (submitBtn) { submitBtn.disabled = true; submitBtn.textContent = 'Verifying...'; }

    /** Shared success handler */
    function _onLoginSuccess() {
        isAuthenticated = true;
        sessionStorage.setItem('authSession', JSON.stringify({ user: username, ts: Date.now() }));
        applyAuthState();
        closeLoginModal();
        showToast(`Welcome, ${username}!`);
    }

    /** Client-side SHA-256 fallback (used when Worker is unreachable) */
    async function _tryLocalFallback() {
        try {
            const hash = await sha256(`${username}:${password}`);
            if (hash === _AUTH_HASH) {
                _onLoginSuccess();
                return true;
            }
        } catch (e) { console.warn('Local fallback hash check failed:', e); }
        return false;
    }

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8000); // 8s timeout
        const resp = await fetch(`${CONFIG.WORKER_URL}/api/auth`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
            signal: controller.signal,
        });
        clearTimeout(timeout);

        // Cloudflare Bot Fight Mode returns non-JSON 403/1010 responses
        const contentType = resp.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            console.warn('Worker returned non-JSON (likely CF Bot Fight Mode), trying local fallback');
            const ok = await _tryLocalFallback();
            if (!ok && errEl) {
                errEl.textContent = 'Invalid username or password.';
                errEl.classList.remove('hidden');
            }
            return;
        }

        const data = await resp.json().catch(() => ({}));

        if (resp.ok && data.success) {
            _onLoginSuccess();
        } else {
            if (errEl) {
                errEl.textContent = data.error || 'Invalid username or password.';
                errEl.classList.remove('hidden');
            }
        }
    } catch (err) {
        console.warn('Worker auth request failed, trying local fallback:', err.message);
        // Network error, CORS block, timeout, etc. → try client-side fallback
        const ok = await _tryLocalFallback();
        if (!ok && errEl) {
            errEl.textContent = 'Invalid username or password.';
            errEl.classList.remove('hidden');
        }
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.innerHTML = '<svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1"/></svg> Log In';
        }
    }
}

function doLogout() {
    isAuthenticated = false;
    sessionStorage.removeItem('authSession');
    applyAuthState();
    showToast('Logged out');
}

// ── Spinner helpers ────────────────────────────────────────────────────
function hideSpinner() {
    const el = document.getElementById('loadingSpinner');
    if (el) {
        el.classList.add('hidden');
        el.style.display = 'none'; // Absolute fallback
    }
}

// ── Initialize ─────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    // If agGrid CDN script failed to load, show error and bail out immediately
    if (typeof agGrid === 'undefined') {
        hideSpinner();
        const el = document.getElementById('loadingSpinner');
        if (el) {
            el.innerHTML = '<div class="text-center p-8"><p class="text-red-600 font-bold text-lg">⚠️ Failed to load AG Grid from CDN.</p><p class="text-gray-500 text-sm mt-2">Check your internet connection and refresh the page.</p></div>';
            el.classList.remove('hidden');
            el.style.display = 'flex';
        }
        return;
    }

    // Safety net: hide spinner after 4 seconds no matter what
    const spinnerTimer = setTimeout(() => {
        hideSpinner();
        try { switchTab('overview'); } catch(e) {}
        try { showToast('Dashboard loaded', 'info'); } catch(e) {}
    }, 4000);

    initTheme();
    initAuth();
    try { initLeadGrid(); } catch(e) { console.error('initLeadGrid failed:', e); }
    try { initHealthGrid(); } catch(e) { console.error('initHealthGrid failed:', e); }
    try { initHistoricalGrid(); } catch(e) { console.error('initHistoricalGrid failed:', e); }

    loadData().finally(() => clearTimeout(spinnerTimer));

    // Wire up global search
    const searchInput = document.getElementById('globalSearch');
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            globalSearchTerm = searchInput.value;
            applyFilters();
        });
    }
});

// ── Theme ──────────────────────────────────────────────────────────────
function initTheme() {
    const saved = localStorage.getItem('theme');
    if (saved === 'dark') document.documentElement.setAttribute('data-theme', 'dark');
}

function toggleTheme() {
    const html = document.documentElement;
    const current = html.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
    // Re-render charts for theme
    if (recentLeads.length > 0) renderCharts();
}

function isDark() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

// ── Data Loading ───────────────────────────────────────────────────────
async function loadData(isRefresh = false) {
    try {
        await Promise.all([loadLeads(), loadJobRuns(), loadScoringRules()]);
    } catch (err) {
        console.error('loadData error:', err);
        try { loadDemoData(); } catch(e) { console.error('Demo leads failed:', e); }
        try { loadDemoHealthData(); } catch(e) { console.error('Demo health failed:', e); }
    } finally {
        hideSpinner();
        _updateLastSyncChip();
        // Only switch to overview on the initial page load, not on manual refresh
        if (!isRefresh) {
            try { switchTab('overview'); } catch(e) { console.error('switchTab failed:', e); }
        }
    }
}

/** Update the "Last pipeline sync" chip in the nav bar.
 *  Uses the most recent finished_at from job_runs (real pipeline time).
 */
function _updateLastSyncChip() {
    const chip = document.getElementById('lastSyncChip');
    const text = document.getElementById('lastSyncText');
    if (!chip || !text) return;

    // Find the latest finished_at across all job runs already in memory
    let latestFinished = null;
    let fallbackFinished = null;
    if (allJobRuns && allJobRuns.length > 0) {
        for (const run of allJobRuns) {
            if (run.finished_at) {
                const t = new Date(run.finished_at);
                // Keep track of the most recent completed job as a fallback
                if (!fallbackFinished || t > fallbackFinished) fallbackFinished = t;

                // ONLY consider the actual overarching pipeline sync jobs primarily
                if (run.source_name === 'pipeline_sync') {
                    if (!latestFinished || t > latestFinished) latestFinished = t;
                }
            }
        }
    }

    // Fallback if the new pipeline_sync event hasn't executed yet
    if (!latestFinished && fallbackFinished) {
        latestFinished = fallbackFinished;
    }

    _renderSyncChip(latestFinished);
}

/** Render the sync chip text given a Date (or null). */
function _renderSyncChip(latestFinished) {
    const chip = document.getElementById('lastSyncChip');
    const text = document.getElementById('lastSyncText');
    if (!chip || !text) return;

    if (latestFinished) {
        const now = new Date();
        const isToday = latestFinished.toDateString() === now.toDateString();
        const timeStr = latestFinished.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true });
        text.textContent = isToday
            ? `Last synced: ${timeStr}`
            : `Last synced: ${latestFinished.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} at ${timeStr}`;
    } else {
        text.textContent = 'Awaiting first sync';
    }
    chip.classList.remove('hidden');
}

/** Lightweight background poll — fetches only the latest finished_at from Supabase.
 *  Runs every 3 minutes. No auth needed (anon key can read job_runs).
 *  Updates the chip silently without reloading any lead data.
 */
async function _pollSyncTime() {
    if (!supabaseClient) return;
    try {
        const { data, error } = await supabaseClient
            .from('job_runs')
            .select('finished_at')
            .not('finished_at', 'is', null)
            .order('finished_at', { ascending: false })
            .limit(1);
        if (error || !data || data.length === 0) return;
        const latest = new Date(data[0].finished_at);
        // Only update if this is newer than what we already show
        _renderSyncChip(latest);
    } catch (e) {
        // Silently ignore — non-critical background poll
    }
}

/**
 * loadLeads() — Two separate queries, merged client-side:
 *  1. Recent      → ALL sources WHERE permit_date >= 90 days ago
 *  2. Historical  → ALL records paginated → populates allLeads (Historical tab only)
 *
 * Recent results are sorted by permit_date desc.
 */
async function loadLeads() {
    if (!supabaseClient) {
        loadDemoData();
        return;
    }

    try {
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - 90);
        const cutoffDate = cutoff.toISOString().slice(0, 10); // YYYY-MM-DD

        const pageSize = 1000;

        /** Helper: paginate through any Supabase query builder and return all rows */
        async function fetchAllPages(buildQuery) {
            const rows = [];
            let offset = 0;
            while (true) {
                const { data: page, error } = await buildQuery(offset, offset + pageSize - 1);
                if (error) throw error;
                if (page && page.length > 0) rows.push(...page);
                if (!page || page.length < pageSize || rows.length >= 25000) break;
                offset += pageSize;
            }
            return rows;
        }

        // ── Query 1: Recent leads — permits issued in the last 90 days ──
        // Filters by permit_date (city issuance date) so we show genuinely recent
        // permits. Sorted by permit_date desc so the newest permits come first.
        // Note: leads with no address (e.g. DERM no-address) are intentionally included
        // so they appear in score distribution charts. The map skips them via getLeadCoords().
        const allRecent = await fetchAllPages((from, to) =>
            supabaseClient
                .from('leads')
                .select('*')
                .gte('permit_date', cutoffDate)
                .order('permit_date', { ascending: false })
                .range(from, to)
        );

        recentLeads = allRecent;

        // ── Query 2: All leads for Historical tab (paginated, no date filter) ──
        allLeads = await fetchAllPages((from, to) =>
            supabaseClient
                .from('leads')
                .select('*')
                .order('permit_date', { ascending: false })
                .range(from, to)
        );

        onLeadsLoaded();
    } catch (err) {
        console.warn('loadLeads failed, using demo:', err.message || err);
        loadDemoData();
    }
}

async function loadJobRuns() {
    if (!supabaseClient) {
        loadDemoHealthData();
        return;
    }

    try {
        const { data, error } = await supabaseClient
            .from('job_runs')
            .select('*')
            .order('started_at', { ascending: false })
            .limit(50);

        if (error) throw error;

        allJobRuns = data || [];
        if (healthGridApi) healthGridApi.setGridOption('rowData', allJobRuns);
        renderHealthCards(allJobRuns);
    } catch (err) {
        console.warn('loadJobRuns failed, using demo:', err.message || err);
        loadDemoHealthData();
    }
}

function onLeadsLoaded() {
    // recentLeads is already filtered to last 90 days by discovered_at with a valid address
    // (server-side filter applied in loadLeads — no client-side re-filtering needed)

    if (leadGridApi) leadGridApi.setGridOption('rowData', recentLeads);
    updateStats();
    renderCharts();
    renderRecentLeads();
    renderHotLeads();
    syncOverviewPanelHeight();
    updateNotifications();
    const badge = document.getElementById('leadCountBadge');
    if (badge) badge.textContent = recentLeads.length;

    // Also populate the Historical tab grid with ALL leads (no filter)
    renderHistoricalData();
}

// ── Notification Bell ──────────────────────────────────────────────────
function updateNotifications() {
    const lastSeen = localStorage.getItem('notifLastSeen') || '1970-01-01T00:00:00Z';
    const newLeads = recentLeads.filter(l => l.discovered_at && l.discovered_at > lastSeen);
    const badge = document.getElementById('notifBadge');
    if (badge) {
        if (newLeads.length > 0) {
            badge.textContent = newLeads.length > 99 ? '99+' : newLeads.length;
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }
    }
    // Render the dropdown list
    const list = document.getElementById('notifList');
    if (!list) return;
    if (newLeads.length === 0) {
        list.innerHTML = '<p class="text-sm text-gray-400 text-center py-6">No new leads</p>';
        return;
    }
    const top20 = newLeads
        .sort((a, b) => (b.discovered_at || '').localeCompare(a.discovered_at || ''))
        .slice(0, 20);
    list.innerHTML = top20.map(lead => {
        const ago = timeAgo(lead.discovered_at);
        return `
            <div class="px-4 py-3 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer transition-colors" onclick="openDetailById('${lead.id}')">
                <div class="flex items-center justify-between">
                    <span class="text-sm font-medium text-gray-800 dark:text-gray-200 truncate flex-1">${lead.address || '—'}</span>
                    ${scoreRenderer({value: computeLeadScore(lead), data: lead})}
                </div>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">${formatSourceName(lead.source_name)} · ${ago}</p>
            </div>`;
    }).join('');
    if (newLeads.length > 20) {
        list.innerHTML += `<p class="text-xs text-gray-400 text-center py-2">+ ${newLeads.length - 20} more</p>`;
    }
}

function timeAgo(isoStr) {
    if (!isoStr) return '';
    const diff = Date.now() - new Date(isoStr).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
}

function toggleNotificationPanel() {
    if (!requireAuth('view notifications')) return;
    const panel = document.getElementById('notifPanel');
    if (panel) panel.classList.toggle('hidden');
}

function markAllNotifSeen() {
    if (!requireAuth('clear notifications')) return;
    localStorage.setItem('notifLastSeen', new Date().toISOString());
    updateNotifications();
    const panel = document.getElementById('notifPanel');
    if (panel) panel.classList.add('hidden');
    showToast('Notifications cleared', 'info');
}

// Close notification panel when clicking outside
document.addEventListener('click', (e) => {
    const panel = document.getElementById('notifPanel');
    const btn = document.getElementById('notifBellBtn');
    if (panel && btn && !panel.contains(e.target) && !btn.contains(e.target)) {
        panel.classList.add('hidden');
    }
});

// ── Refresh with cooldown ──────────────────────────────────────────────
const REFRESH_COOLDOWN_MS = 30_000; // 30 seconds
const PIPELINE_COOLDOWN_MS = 15 * 60 * 1000; // 15 minutes
let lastRefreshAt = 0;
let refreshCooldownTimer = null;

async function refreshData() {
    if (!requireAuth('refresh data')) return;

    const now = Date.now();
    const elapsed = now - lastRefreshAt;

    if (elapsed < REFRESH_COOLDOWN_MS && lastRefreshAt > 0) {
        const remaining = Math.ceil((REFRESH_COOLDOWN_MS - elapsed) / 1000);
        showToast(`Please wait ${remaining}s before refreshing again`, 'error');
        return;
    }

    lastRefreshAt = now;
    _startRefreshCooldownUI();
    showToast('Refreshing local data...', 'info');
    await loadData(true);

    // Then handle the 15-minute pipeline trigger
    let lastPipelineTriggerAt = parseInt(localStorage.getItem('lastPipelineTriggerAt') || '0', 10);
    const pipelineElapsed = now - lastPipelineTriggerAt;

    if (pipelineElapsed >= PIPELINE_COOLDOWN_MS || lastPipelineTriggerAt === 0) {
        const workerUrl = CONFIG.WORKER_URL;
        if (workerUrl && !workerUrl.includes('YOUR_SUBDOMAIN')) {
            showToast('Triggering full pipeline synchronization from source servers...', 'info');
            try {
                const resp = await fetch(`${workerUrl}/api/dispatch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ workflow: 'daily_pipeline.yml' })
                });

                if (resp.ok) {
                    localStorage.setItem('lastPipelineTriggerAt', now.toString());
                    showToast('Pipeline explicitly triggered! New leads will arrive in ~2-5 mins.', 'success', 8000);
                } else {
                    const errText = await resp.text();
                    console.error('Failed to trigger daily pipeline:', errText);
                    showToast(`Pipeline trigger failed: ${errText.slice(0, 50)}`, 'error', 8000);
                }
            } catch (err) {
                console.error('Dispatch trigger error', err);
                showToast('Pipeline trigger error (see console)', 'error', 8000);
            }
        }
    } else {
        const minutesLeft = Math.ceil((PIPELINE_COOLDOWN_MS - pipelineElapsed) / 60000);
        showToast(`Database refreshed. Source sync cooldown: ~${minutesLeft} mins.`, 'success');
    }
}

/** Grey out the refresh button and show a countdown during the cooldown period */
function _startRefreshCooldownUI() {
    const btn = document.querySelector('button[onclick="refreshData()"]');
    if (!btn) return;

    clearInterval(refreshCooldownTimer);
    btn.disabled = true;
    btn.style.opacity = '0.45';
    btn.title = 'Refreshing...';

    let remaining = Math.ceil(REFRESH_COOLDOWN_MS / 1000);
    refreshCooldownTimer = setInterval(() => {
        remaining--;
        if (remaining <= 0) {
            clearInterval(refreshCooldownTimer);
            btn.disabled = false;
            btn.style.opacity = '';
            btn.title = 'Refresh Data';
        } else {
            btn.title = `Refresh available in ${remaining}s`;
        }
    }, 1000);
}

// ── Lead Grid ──────────────────────────────────────────────────────────
function initLeadGrid() {
    const columnDefs = [
        {
            headerCheckboxSelection: true,
            checkboxSelection: true,
            width: 40, maxWidth: 40, pinned: 'left',
            suppressMenu: true, resizable: false, suppressSizeToFit: true,
        },
        {
            field: 'lead_score', headerName: 'Score',
            width: 135, minWidth: 135, maxWidth: 150, suppressSizeToFit: true,
            cellStyle: { textAlign: 'center' },
            valueGetter: (params) => computeLeadScore(params.data),
            cellRenderer: scoreRenderer,
            sort: 'desc', sortIndex: 0,
            comparator: (valA, valB, nodeA, nodeB) => {
                if (valA !== valB) return valA - valB;
                const pnA = (nodeA.data && nodeA.data.permit_number) || '';
                const pnB = (nodeB.data && nodeB.data.permit_number) || '';
                return pnA.localeCompare(pnB);
            },
        },
        {
            field: 'address', headerName: 'Address',
            minWidth: 130, flex: 1,
            cellRenderer: addressRenderer,
        },
        {
            field: 'owner_name', headerName: 'Owner',
            width: 140, minWidth: 110,
            cellStyle: { fontSize: '12px' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'permit_type', headerName: 'Permit Type',
            minWidth: 110, flex: 0.7,
            cellStyle: { fontSize: '12px' },
        },
        {
            field: 'permit_description', headerName: 'Description',
            width: 160, minWidth: 120,
            cellStyle: { fontSize: '11px', color: '#6b7280' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'permit_date', headerName: 'Date',
            width: 100, minWidth: 90,
            valueFormatter: (p) => p.value ? new Date(p.value).toLocaleDateString() : '—',
        },
        {
            field: 'jurisdiction', headerName: 'Jurisdiction',
            width: 130, minWidth: 110,
        },
        {
            field: 'source_name', headerName: 'Source',
            width: 130, minWidth: 110,
            valueFormatter: (p) => formatSourceName(p.value),
        },
        {
            field: 'contractor_name', headerName: 'Contractor',
            width: 140, minWidth: 110,
            cellStyle: { fontSize: '12px' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'contractor_phone', headerName: 'Contr. Phone',
            width: 120, minWidth: 100,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="tel:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'owner_phone', headerName: 'Owner Phone',
            width: 120, minWidth: 100,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="tel:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'owner_email', headerName: 'Owner Email',
            width: 160, minWidth: 120,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="mailto:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'lead_status', headerName: 'Status',
            width: 100, minWidth: 90,
            cellRenderer: statusRenderer,
        },
        {
            headerName: 'Actions', width: 110, minWidth: 100,
            pinned: 'right', suppressSizeToFit: true,
            cellRenderer: actionsRenderer,
            suppressMenu: true, sortable: false, filter: false,
        },
    ];

    const gridOptions = {
        columnDefs,
        rowData: [],
        rowSelection: 'multiple',
        suppressRowClickSelection: true,
        animateRows: true,
        pagination: true,
        paginationPageSize: 50,
        paginationPageSizeSelector: [25, 50, 100, 200],
        defaultColDef: { sortable: true, filter: true, resizable: true },
        getRowClass: (params) => {
            if (params.data && params.data.lead_status === 'new') return 'lead-new';
            return '';
        },
        onSelectionChanged: () => {
            const count = leadGridApi.getSelectedRows().length;
            console.log(`${count} selected`);
        },
        onRowDoubleClicked: (e) => openDetail(e.data),
        isExternalFilterPresent: () => true,
        doesExternalFilterPass: doesFilterPass,
        onFirstDataRendered: () => _injectScoreInfoBtn(gridDiv),
    };

    const gridDiv = document.getElementById('leadGrid');
    leadGridApi = agGrid.createGrid(gridDiv, gridOptions);
}

// ── Score Legend Popover (dynamic — reads scoringRules) ─────────────────
function _buildScorePopoverHTML() {
    const r = scoringRules;
    return `
<div id="scorePopover" class="score-popover" onmouseleave="hideScorePopover()">
    <h4>📊 Lead Score Legend</h4>
    <div class="score-popover-row">
        <span class="score-popover-badge sp-high">7 – 10+</span>
        <span class="score-popover-label">🔥 <strong>Hot</strong> — Tree removal / arbor permit, direct opportunity</span>
    </div>
    <div class="score-popover-row">
        <span class="score-popover-badge sp-med">4 – 6</span>
        <span class="score-popover-label">⚡ <strong>Warm</strong> — Vegetation / partial match · 🤝 Partnership if contractor assigned</span>
    </div>
    <div class="score-popover-row">
        <span class="score-popover-badge sp-low">0 – 3</span>
        <span class="score-popover-label">📋 <strong>Low</strong> — General permit, few signals</span>
    </div>
    <div class="score-popover-calc">
        <strong>How scores are calculated:</strong><br>
        +${r.tree_removal_bonus} pts — Tree removal / arbor permit<br>
        +${r.vegetation_removal_bonus} pts — Vegetation removal permit<br>
        +${r.recency_bonus} pts — Filed in the last ${r.recency_days_threshold} days (non-DERM)<br>
        +${r.derm_tier1_bonus}/+${r.derm_tier2_bonus}/+${r.derm_tier3_bonus} pts — DERM tiered recency (${r.derm_tier1_days_min}–${r.derm_tier1_days_max}/${r.derm_tier2_days_min}–${r.derm_tier2_days_max}/${r.derm_tier3_days_min}–${r.derm_tier3_days_max} days)<br>
        +${r.large_parcel_bonus} pts — Parcel &gt; ${r.parcel_acres_threshold} acres<br>
        +${r.right_of_way_bonus} pt&nbsp; — Right-of-way permit<br>
        −${r.intended_decision_penalty} pt&nbsp; — "Intended Decision" status (City of Miami Tree)<br>
        −${r.contractor_penalty} pts — Contractor already assigned (Partnership Opp.)<br>
        −${r.derm_no_address_penalty} pts — DERM permit with no address
    </div>
</div>`;
}

let _scorePopoverEl = null;

function _ensureScorePopover() {
    if (!_scorePopoverEl) {
        document.body.insertAdjacentHTML('beforeend', _buildScorePopoverHTML());
        _scorePopoverEl = document.getElementById('scorePopover');
    }
    return _scorePopoverEl;
}

/** Rebuild popover content (called after scoring rules change) */
function _rebuildScorePopover() {
    if (_scorePopoverEl) { _scorePopoverEl.remove(); _scorePopoverEl = null; }
}

function showScorePopover(evt) {
    const pop = _ensureScorePopover();
    pop.classList.add('show');
    const btn = evt.currentTarget;
    const rect = btn.getBoundingClientRect();
    let left = rect.right + 8;
    let top = rect.top - 4;
    // Keep within viewport
    if (left + 350 > window.innerWidth) left = rect.left - 350;
    if (top + 300 > window.innerHeight) top = Math.max(8, window.innerHeight - 310);
    pop.style.left = left + 'px';
    pop.style.top = top + 'px';
}

function hideScorePopover() {
    if (_scorePopoverEl) _scorePopoverEl.classList.remove('show');
}

// Close popover on any click outside
document.addEventListener('click', (e) => {
    if (_scorePopoverEl && !e.target.closest('.score-popover') && !e.target.closest('.score-info-btn')) {
        hideScorePopover();
    }
});

function _scoreHeaderWithInfo(label) {
    return `<span class="score-info-wrap">${label}<button class="score-info-btn" onclick="event.stopPropagation();showScorePopover(event)" onmouseenter="showScorePopover(event)" title="Score legend">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" width="14" height="14">
            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
        </svg>
    </button></span>`;
}

/** Inject an ⓘ button into all AG Grid "Score" column headers inside a container */
function _injectScoreInfoBtn(container) {
    if (!container) return;
    setTimeout(() => {
        container.querySelectorAll('.ag-header-cell-text').forEach(el => {
            if (el.textContent.trim() === 'Score' && !el.querySelector('.score-info-btn')) {
                const btn = document.createElement('button');
                btn.className = 'score-info-btn';
                btn.title = 'Score legend';
                btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" width="14" height="14">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                </svg>`;
                btn.onclick = (e) => { e.stopPropagation(); showScorePopover(e); };
                btn.onmouseenter = (e) => showScorePopover(e);
                el.style.display = 'inline-flex';
                el.style.alignItems = 'center';
                el.style.gap = '4px';
                el.appendChild(btn);
            }
        });
    }, 200);
}

// ── Live Lead Scoring (mirrors pipeline/scoring.py — uses scoringRules) ──
/**
 * Compute the lead score dynamically from the lead's fields.
 * Uses the globally-loaded scoringRules object (loaded from DB or defaults).
 * Scoring logic is identical to pipeline/scoring.py.
 */
function computeLeadScore(lead) {
    if (!lead) return 0;
    const r = scoringRules;

    const TREE_REMOVAL_TYPES = new Set([
        'TREE REMOVAL', 'TREE REMOVAL PERMIT', 'TREE PERMIT',
        'ARBOR PERMIT', 'TREE ALTERATION', 'LANDSCAPE TREE REMOVAL-RELOCATION PERMIT',
    ]);
    const VEGETATION_TYPES = new Set(['VEGETATION REMOVAL']);
    const LANDSCAPE_TYPES = new Set(['LANDSCAPE INSTALLATION PERMIT', 'ROW LANDSCAPING PERMIT']);
    const ROW_KEYWORDS = ['right of way', 'right-of-way', 'row ', 'r.o.w.'];
    const DERM_SOURCES = new Set(['miami_dade_derm']);

    const ptUpper = (lead.permit_type || '').trim().toUpperCase();
    const descLower = (lead.permit_description || '').toLowerCase();

    let score = 0;

    // Tree removal / arbor bonus
    if (TREE_REMOVAL_TYPES.has(ptUpper)) {
        score += r.tree_removal_bonus;
    } else if (VEGETATION_TYPES.has(ptUpper)) {
        score += r.vegetation_removal_bonus;
    } else if (LANDSCAPE_TYPES.has(ptUpper)) {
        score += r.landscape_installation_bonus;
    }

    // Description fallback (only when type gave nothing)
    if (score === 0) {
        if (['tree removal', 'remove tree', 'arbor', 'landscape tree removal', 'tree relocation']
                .some(kw => descLower.includes(kw))) {
            score += r.tree_removal_bonus;
        } else if (['vegetation removal', 'remove vegetation'].some(kw => descLower.includes(kw))) {
            score += r.vegetation_removal_bonus;
        } else if (['dead tree', 'dangerous tree'].some(kw => descLower.includes(kw))) {
            score += r.tree_removal_bonus;
        } else if (['landscape installation', 'row landscaping'].some(kw => descLower.includes(kw))) {
            score += r.landscape_installation_bonus;
        }
    }

    // ── Recency / DERM tiered recency ──────────────────────────────────
    if (lead.permit_date) {
        const permitDt = new Date(lead.permit_date);
        if (!isNaN(permitDt)) {
            const ageDays = Math.floor((Date.now() - permitDt.getTime()) / 86400000);
            const isDerm = DERM_SOURCES.has((lead.source_name || '').toLowerCase());
            if (isDerm) {
                if (ageDays >= r.derm_tier1_days_min && ageDays <= r.derm_tier1_days_max) {
                    score += r.derm_tier1_bonus;
                } else if (ageDays >= r.derm_tier2_days_min && ageDays <= r.derm_tier2_days_max) {
                    score += r.derm_tier2_bonus;
                } else if (ageDays >= r.derm_tier3_days_min && ageDays <= r.derm_tier3_days_max) {
                    score += r.derm_tier3_bonus;
                }
            } else {
                // General tiered recency
                const rt1 = r.recency_tier1_days_max;
                const rt2 = r.recency_tier2_days_max;
                const rt3 = r.recency_tier3_days_max;
                if (ageDays <= rt1) {
                    score += r.recency_tier1_bonus;
                } else if (ageDays <= rt2) {
                    score += r.recency_tier2_bonus;
                } else if (ageDays <= rt3) {
                    score += r.recency_tier3_bonus;
                }
            }

            // Staleness penalty (>365 days)
            const staleDays = r.staleness_days_threshold;
            const stalePen  = r.staleness_penalty;
            if (ageDays > staleDays) {
                score = Math.max(0, score - stalePen);
            }
        }
    }

    // Right-of-way bonus
    if (ROW_KEYWORDS.some(kw => descLower.includes(kw)) || ptUpper.includes('RIGHT OF WAY')) {
        score += r.right_of_way_bonus;
    }

    // Contractor penalty
    if ((lead.contractor_name || '').trim()) {
        score = Math.max(0, score - r.contractor_penalty);
    }

    // DERM no-address penalty
    if (DERM_SOURCES.has((lead.source_name || '').toLowerCase()) && !(lead.address || '').trim()) {
        score = Math.max(0, score - r.derm_no_address_penalty);
    }

    // "Intended Decision" penalty (City of Miami Tree)
    if (
        (lead.source_name || '').toLowerCase() === 'city_of_miami_tree' &&
        (lead.permit_status || '').trim().toLowerCase() === 'intended decision'
    ) {
        score = Math.max(0, score - r.intended_decision_penalty);
    }

    // "Corrections Required" penalty (application has issues — not actionable yet)
    if ((lead.permit_status || '').trim().toLowerCase() === 'corrections required') {
        score = Math.max(0, score - r.corrections_required_penalty);
    }

    return score;
}

// ── Cell Renderers ─────────────────────────────────────────────────────
function scoreRenderer(params) {
    const score = params.value != null ? params.value : computeLeadScore(params.data);
    let cls = 'score-low';
    if (score >= 7) cls = 'score-high';
    else if (score >= 4) cls = 'score-medium';
    return `<span class="score-badge ${cls}">${score}</span>`;
}

function addressRenderer(params) {
    const addr = params.value || '—';
    return `<span style="cursor:pointer;color:var(--ag-foreground-color)" class="hover:underline" onclick="openDetailById('${params.data.id}')">${addr}</span>`;
}

function statusRenderer(params) {
    const status = params.value || 'new';
    return `<span class="status-badge status-${status}">${status}</span>`;
}

function actionsRenderer(params) {
    const id = params.data.id;
    return `
        <span class="row-action-btn row-action-approve" title="Approve" onclick="event.stopPropagation(); updateLeadStatus('${id}','approved')">✓</span>
        <span class="row-action-btn row-action-reject" title="Reject" onclick="event.stopPropagation(); updateLeadStatus('${id}','rejected')">✕</span>
    `;
}

function formatSourceName(name) {
    const map = {
        'miami_dade_derm': 'Miami-Dade DERM',
        'derm_tree': 'DERM Tree Permits',
        'fort_lauderdale': 'Fort Lauderdale',
        'city_of_miami': 'City of Miami',
        'city_of_miami_tree': 'Miami Tree Permits',
    };
    return map[name] || name || '—';
}

// ── Filters ────────────────────────────────────────────────────────────
function doesFilterPass(node) {
    const data = node.data;
    if (!data) return false;

    const dateFrom = document.getElementById('filterDateFrom').value;
    const dateTo = document.getElementById('filterDateTo').value;
    const jurisdiction = document.getElementById('filterJurisdiction').value;
    const status = document.getElementById('filterStatus').value;
    const minScore = document.getElementById('filterMinScore').value;

    if (dateFrom && data.permit_date && data.permit_date < dateFrom) return false;
    if (dateTo && data.permit_date && data.permit_date > dateTo) return false;
    if (jurisdiction && data.jurisdiction !== jurisdiction) return false;
    if (status && data.lead_status !== status) return false;
    if (minScore && computeLeadScore(data) < parseInt(minScore)) return false;

    // Global search
    if (globalSearchTerm) {
        const search = globalSearchTerm.toLowerCase();
        const haystack = [data.address, data.permit_type, data.permit_description, data.contractor_name, data.contractor_phone, data.owner_name, data.owner_phone, data.owner_email, data.permit_number, data.jurisdiction]
            .filter(Boolean).join(' ').toLowerCase();
        if (!haystack.includes(search)) return false;
    }

    return true;
}

function applyFilters() {
    if (leadGridApi) leadGridApi.onFilterChanged();
}

// Alias for HTML onchange handlers
const onFilterChanged = applyFilters;

function clearFilters() {
    document.getElementById('filterDateFrom').value = '';
    document.getElementById('filterDateTo').value = '';
    document.getElementById('filterJurisdiction').value = '';
    document.getElementById('filterStatus').value = '';
    document.getElementById('filterMinScore').value = '';
    document.getElementById('globalSearch').value = '';
    globalSearchTerm = '';
    applyFilters();
}

function globalSearchFilter() {
    globalSearchTerm = document.getElementById('globalSearch').value;
    applyFilters();
}

// ── Lead Actions ───────────────────────────────────────────────────────
async function updateLeadStatus(id, status) {
    if (!requireAuth('update lead status')) return;
    try {
        const lead = allLeads.find(l => String(l.id) === String(id)) || recentLeads.find(l => String(l.id) === String(id));
        if (supabaseClient && lead) {
            const { error } = await supabaseClient.from('leads').update({ lead_status: status }).eq('id', lead.id);
            if (error) throw error;
        }
        if (lead) {
            lead.lead_status = status;
            leadGridApi.applyTransaction({ update: [lead] });
            updateStats();
        }
        showToast(`Lead ${status}`);
    } catch (err) {
        console.error('Failed to update lead:', err);
        showToast('Failed to update lead', 'error');
    }
}

async function bulkAction(status) {
    if (!requireAuth('perform bulk actions')) return;
    const selected = leadGridApi.getSelectedRows();
    if (selected.length === 0) { showToast('No leads selected'); return; }

    try {
        if (supabaseClient) {
            const ids = selected.map(r => r.id);
            if (ids.length) {
                const { error } = await supabaseClient.from('leads').update({ lead_status: status }).in('id', ids);
                if (error) throw error;
            }
        }
        selected.forEach(lead => { lead.lead_status = status; });
        leadGridApi.applyTransaction({ update: selected });
        leadGridApi.deselectAll();
        updateStats();
        showToast(`${selected.length} leads ${status}`);
    } catch (err) {
        console.error('Bulk action failed:', err);
        showToast('Bulk action failed', 'error');
    }
}

function bulkApprove() { bulkAction('approved'); }
function bulkReject() { bulkAction('rejected'); }

// ── Export ──────────────────────────────────────────────────────────────
function exportCSV() {
    if (!requireAuth('export data')) return;
    if (leadGridApi) {
        leadGridApi.exportDataAsCsv({
            fileName: `tree-permits-${new Date().toISOString().slice(0,10)}.csv`,
            suppressBOM: true,
            columnKeys: ['owner_name','owner_phone','owner_email','address','permit_type','permit_description','permit_number','permit_date','jurisdiction','source_name','lead_score','lead_status','contractor_name','contractor_phone','source_url'],
        });
        showToast('CSV exported');
    }
}

function exportSelected() {
    if (!requireAuth('export data')) return;
    const selected = leadGridApi.getSelectedRows();
    if (selected.length === 0) { showToast('No leads selected'); return; }

    selected.forEach(lead => { lead.lead_status = 'exported'; });
    leadGridApi.applyTransaction({ update: selected });

    leadGridApi.exportDataAsCsv({
        fileName: `tree-permits-selected-${new Date().toISOString().slice(0,10)}.csv`,
        onlySelected: true,
        suppressBOM: true,
        columnKeys: ['owner_name','owner_phone','owner_email','address','permit_type','permit_description','permit_number','permit_date','jurisdiction','source_name','lead_score','lead_status','contractor_name','contractor_phone','source_url'],
    });

    // Mark as exported in db
    if (supabaseClient) {
        try {
            supabaseClient.from('leads').update({ lead_status: 'exported' }).in('id', ids).then();
        } catch(e){}
    }

    leadGridApi.deselectAll();
    updateStats();
    showToast(`${selected.length} leads exported`);
}

function exportHistoricalCSV() {
    if (!requireAuth('export data')) return;
    if (historicalGridApi) {
        historicalGridApi.exportDataAsCsv({
            fileName: `tree-permits-historical-${new Date().toISOString().slice(0,10)}.csv`,
            suppressBOM: true,
            columnKeys: ['owner_name','owner_phone','owner_email','address','permit_type','permit_description','permit_number','permit_date','jurisdiction','source_name','lead_score','lead_status','contractor_name','contractor_phone','source_url'],
        });
        showToast('Historical CSV exported');
    }
}

// ── Detail Modal ───────────────────────────────────────────────────────
function openDetail(lead) {
    currentDetailLead = lead;
    const rawJson = lead.raw_payload_json
        ? (typeof lead.raw_payload_json === 'string' ? JSON.parse(lead.raw_payload_json) : lead.raw_payload_json)
        : {};

    const html = `
        <div class="space-y-0">
            <div class="detail-row"><span class="detail-label">Address</span><span class="detail-value font-semibold">${lead.address || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Jurisdiction</span><span class="detail-value">${lead.jurisdiction || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Permit Type</span><span class="detail-value">${lead.permit_type || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Description</span><span class="detail-value">${lead.permit_description || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Permit #</span><span class="detail-value font-mono">${lead.permit_number || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Permit Date</span><span class="detail-value">${lead.permit_date ? new Date(lead.permit_date).toLocaleDateString() : '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Status</span><span class="detail-value"><span class="status-badge status-${lead.lead_status || 'new'}">${lead.lead_status || 'new'}</span></span></div>
            <div class="detail-row"><span class="detail-label">Score</span><span class="detail-value">${scoreRenderer({value: computeLeadScore(lead), data: lead})}</span></div>
            <div class="detail-row"><span class="detail-label">Owner</span><span class="detail-value">${lead.owner_name || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Owner Phone</span><span class="detail-value">${lead.owner_phone ? `<a href="tel:${lead.owner_phone}" class="text-accent-600 hover:underline">${lead.owner_phone}</a>` : '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Owner Email</span><span class="detail-value">${lead.owner_email ? `<a href="mailto:${lead.owner_email}" class="text-accent-600 hover:underline">${lead.owner_email}</a>` : '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Contractor</span><span class="detail-value">${lead.contractor_name || '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Contractor Phone</span><span class="detail-value">${lead.contractor_phone ? `<a href="tel:${lead.contractor_phone}" class="text-accent-600 hover:underline">${lead.contractor_phone}</a>` : '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Source</span><span class="detail-value">${formatSourceName(lead.source_name)}</span></div>
            <div class="detail-row"><span class="detail-label">Source URL</span><span class="detail-value">${lead.source_url && lead.source_url !== '#' ? `<a href="${lead.source_url}" target="_blank" class="text-accent-600 hover:underline text-sm">View Original ↗</a>` : '—'}</span></div>
            <div class="detail-row"><span class="detail-label">Discovered</span><span class="detail-value">${lead.discovered_at ? new Date(lead.discovered_at).toLocaleString() : '—'}</span></div>
        </div>
        <div class="mt-4">
            <div class="raw-json-toggle" onclick="toggleRawJson()">▶ Raw Permit Data</div>
            <div id="rawJsonContent" class="raw-json-content hidden">${JSON.stringify(rawJson, null, 2)}</div>
        </div>
    `;

    document.getElementById('detailContent').innerHTML = html;
    document.getElementById('detailModal').classList.remove('hidden');
}

function openDetailById(id) {
    const lead = allLeads.find(l => String(l.id) === String(id)) || recentLeads.find(l => String(l.id) === String(id));
    if (lead) openDetail(lead);
}

function closeDetail() {
    document.getElementById('detailModal').classList.add('hidden');
    currentDetailLead = null;
}

// Alias for HTML onclick
const closeDetailModal = closeDetail;

function detailAction(status) {
    if (!requireAuth('update lead status')) return;
    if (currentDetailLead) {
        updateLeadStatus(currentDetailLead.id, status);
        closeDetail();
    }
}

function approveCurrentLead() { detailAction('approved'); }
function rejectCurrentLead() { detailAction('rejected'); }

function toggleRawJson() {
    const el = document.getElementById('rawJsonContent');
    const toggle = el.previousElementSibling;
    if (el.classList.contains('hidden')) {
        el.classList.remove('hidden');
        toggle.textContent = '▼ Raw Permit Data';
    } else {
        el.classList.add('hidden');
        toggle.textContent = '▶ Raw Permit Data';
    }
}

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeDetail();
        closeLoginModal();
    }
});

// ── Stats ──────────────────────────────────────────────────────────────
function updateStats() {
    const counts = { total: recentLeads.length, new: 0, approved: 0, rejected: 0, highScore: 0 };
    recentLeads.forEach(l => {
        const s = l.lead_status || 'new';
        if (counts[s] !== undefined) counts[s]++;
        if (computeLeadScore(l) >= 7) counts.highScore++;
    });

    document.getElementById('statTotal').querySelector('p').textContent = counts.total.toLocaleString();
    document.getElementById('statNew').querySelector('p').textContent = counts.new.toLocaleString();
    document.getElementById('statApproved').querySelector('p').textContent = counts.approved.toLocaleString();
    document.getElementById('statRejected').querySelector('p').textContent = counts.rejected.toLocaleString();
    document.getElementById('statHighScore').querySelector('p').textContent = counts.highScore.toLocaleString();
}

// ── Tab Switching ──────────────────────────────────────────────────────
function switchTab(tab) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.add('hidden'));

    const btn = document.querySelector(`.tab-btn[data-tab="${tab}"]`);
    const panel = document.getElementById(`tab-${tab}`);
    if (btn) btn.classList.add('active');
    if (panel) {
        panel.classList.remove('hidden');
        panel.classList.add('fade-in');
    }

    // Lazy init map
    if (tab === 'map' && !leafletMap) {
        setTimeout(() => initMap(), 100);
    }
    if (tab === 'map' && leafletMap) {
        setTimeout(() => leafletMap.invalidateSize(), 100);
    }
    // Re-render historical grid when switching to that tab (AG Grid needs visible container)
    if (tab === 'historical' && historicalGridApi) {
        setTimeout(() => historicalGridApi.sizeColumnsToFit && historicalGridApi.sizeColumnsToFit(), 100);
    }
    // Populate scoring rules UI when switching to that tab
    if (tab === 'scoring') {
        populateScoringRulesUI();
    }
}

// ── Charts (Overview Tab) ──────────────────────────────────────────────
function renderCharts() {
    renderTimelineChart();
    renderFreshnessChart();
    renderScoresChart();
}

// ── Overview panel height sync ─────────────────────────────────────────
function syncOverviewPanelHeight() {
    // On desktop (lg), measure the left charts column and set a CSS variable
    // so the side panels match its height exactly.
    const grid = document.getElementById('overviewGrid');
    if (!grid || window.innerWidth < 1024) return;
    const leftCol = grid.querySelector('.lg\\:col-span-6');
    if (!leftCol) return;
    requestAnimationFrame(() => {
        const h = leftCol.offsetHeight;
        if (h > 0) grid.style.setProperty('--overview-left-h', h + 'px');
    });
}
// Re-sync on resize
window.addEventListener('resize', syncOverviewPanelHeight);

function getChartColors() {
    const dark = isDark();
    return {
        text: dark ? '#94a3b8' : '#64748b',
        grid: dark ? '#1e293b' : '#f1f5f9',
        bg: dark ? '#111827' : '#ffffff',
    };
}

function renderTimelineChart() {
    const ctx = document.getElementById('timelineChart');
    if (!ctx) return;

    // Only include leads from the last 90 days with a valid date
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 90);
    const cutoffStr = cutoff.toISOString().slice(0, 10);

    // Source order & colors — consistent across all charts
    const SOURCE_ORDER = ['miami_dade_derm', 'fort_lauderdale', 'city_of_miami_tree', 'city_of_miami'];
    const SOURCE_COLORS = { miami_dade_derm: '#059669', fort_lauderdale: '#3b82f6', city_of_miami_tree: '#f59e0b', city_of_miami: '#8b5cf6' };

    // Group by ISO week AND source
    const byWeekSrc = {};  // { weekStart: { source: count } }
    recentLeads.forEach(l => {
        const d = l.permit_date ? l.permit_date.slice(0, 10) : null;
        if (!d || d < cutoffStr) return;
        const weekStart = getWeekStart(d);
        if (!byWeekSrc[weekStart]) byWeekSrc[weekStart] = {};
        const src = l.source_name || 'unknown';
        byWeekSrc[weekStart][src] = (byWeekSrc[weekStart][src] || 0) + 1;
    });

    const weeks = Object.keys(byWeekSrc).sort();
    const labels = weeks.map(d => {
        const dt = new Date(d + 'T00:00:00');
        return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    });

    // Build one dataset per source (stacked)
    const datasets = SOURCE_ORDER.map(src => ({
        label: formatSourceName(src),
        data: weeks.map(w => (byWeekSrc[w] && byWeekSrc[w][src]) || 0),
        backgroundColor: SOURCE_COLORS[src] || '#94a3b8',
        borderRadius: 2,
        borderSkipped: false,
    }));

    const colors = getChartColors();
    const maxTotal = Math.max(...weeks.map(w => SOURCE_ORDER.reduce((s, k) => s + ((byWeekSrc[w] && byWeekSrc[w][k]) || 0), 0)), 5);

    if (chartTimeline) chartTimeline.destroy();
    chartTimeline = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { color: colors.text, padding: 12, boxWidth: 12, font: { size: 10 } } },
                tooltip: {
                    mode: 'index',
                    callbacks: {
                        title: (items) => `Week of ${weeks[items[0].dataIndex]}`,
                        footer: (items) => {
                            const total = items.reduce((s, i) => s + i.raw, 0);
                            return `Total: ${total} leads`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    stacked: true,
                    ticks: { color: colors.text, maxTicksLimit: 13, font: { size: 10 } },
                    grid: { display: false },
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    suggestedMax: maxTotal * 1.25,
                    ticks: {
                        color: colors.text,
                        font: { size: 10 },
                        precision: 0,
                        maxTicksLimit: 5,
                    },
                    grid: { color: colors.grid },
                },
            },
        },
    });
}

/** Return the Monday (ISO week start) for a given YYYY-MM-DD string. */
function getWeekStart(dateStr) {
    const d = new Date(dateStr + 'T00:00:00');
    const day = d.getDay();          // 0=Sun … 6=Sat
    const diff = (day === 0 ? 6 : day - 1); // offset to Monday
    d.setDate(d.getDate() - diff);
    return d.toISOString().slice(0, 10);
}

function renderFreshnessChart() {
    const ctx = document.getElementById('freshnessChart');
    if (!ctx) return;

    const SOURCE_ORDER = ['miami_dade_derm', 'fort_lauderdale', 'city_of_miami_tree', 'city_of_miami'];
    const AGE_BUCKETS = [
        { key: '0-30d',   label: '< 30 days',  max: 30,  color: '#22c55e' },
        { key: '31-90d',  label: '31-90 days',  max: 90,  color: '#fbbf24' },
        { key: '91-180d', label: '91-180 days', max: 180, color: '#f97316' },
        { key: '180d+',   label: '> 180 days',  max: Infinity, color: '#ef4444' },
    ];

    const now = Date.now();
    const bySrc = {};
    SOURCE_ORDER.forEach(s => { bySrc[s] = {}; AGE_BUCKETS.forEach(b => bySrc[s][b.key] = 0); });

    recentLeads.forEach(l => {
        const src = l.source_name || 'unknown';
        if (!bySrc[src]) return;
        const pd = l.permit_date ? new Date(l.permit_date) : null;
        if (!pd || isNaN(pd)) return;
        const ageDays = Math.floor((now - pd.getTime()) / 86400000);
        for (const b of AGE_BUCKETS) {
            if (ageDays <= b.max || b.max === Infinity) { bySrc[src][b.key]++; break; }
        }
    });

    const labels = SOURCE_ORDER.map(formatSourceName);
    const datasets = AGE_BUCKETS.map(b => ({
        label: b.label,
        data: SOURCE_ORDER.map(s => bySrc[s][b.key]),
        backgroundColor: b.color,
        borderRadius: 2,
        borderSkipped: false,
    }));

    const colors = getChartColors();
    if (chartFreshness) chartFreshness.destroy();
    chartFreshness = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { color: colors.text, padding: 8, boxWidth: 10, font: { size: 9 } } },
                tooltip: {
                    callbacks: {
                        footer: (items) => {
                            const total = items.reduce((s, i) => s + i.raw, 0);
                            return `Total: ${total}`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    stacked: true,
                    ticks: { color: colors.text, font: { size: 9 }, precision: 0 },
                    grid: { color: colors.grid },
                },
                y: {
                    stacked: true,
                    ticks: { color: colors.text, font: { size: 10 } },
                    grid: { display: false },
                },
            },
        },
    });
}

function renderScoresChart() {
    const ctx = document.getElementById('scoresChart');
    if (!ctx) return;

    const buckets = { '1-3 (Low)': 0, '4-6 (Medium)': 0, '7-9 (High)': 0 };
    recentLeads.forEach(l => {
        const s = computeLeadScore(l);
        if (s >= 7) buckets['7-9 (High)']++;
        else if (s >= 4) buckets['4-6 (Medium)']++;
        else buckets['1-3 (Low)']++;
    });

    const colors = getChartColors();

    if (chartScores) chartScores.destroy();
    chartScores = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: Object.keys(buckets),
            datasets: [{
                label: 'Leads',
                data: Object.values(buckets),
                backgroundColor: ['#e5e7eb', '#fbbf24', '#22c55e'],
                borderRadius: 6,
            }],
        },
        options: {
            indexAxis: 'y',
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { color: colors.text, font: { size: 10 } }, grid: { color: colors.grid } },
                y: { ticks: { color: colors.text, font: { size: 11 } }, grid: { display: false } },
            },
        },
    });
}

// ── Recent Leads (Overview Tab) ────────────────────────────────────────
function renderRecentLeads() {
    const container = document.getElementById('recentLeads');
    if (!container) return;

    const recent = [...recentLeads]
        .sort((a, b) => {
            // Sort by permit issuance date (newest permits first)
            const dateA = a.permit_date || '';
            const dateB = b.permit_date || '';
            return dateB.localeCompare(dateA)
                || (b.permit_number || '').localeCompare(a.permit_number || '');
        })
        .slice(0, 20);

    if (recent.length === 0) {
        container.innerHTML = '<p class="text-sm text-gray-500 py-4 text-center">No recent leads yet.</p>';
        return;
    }

    container.innerHTML = recent.map(lead => {
        const isNew = lead.lead_status === 'new';
        return `
            <div class="record-item ${isNew ? 'record-item-new' : ''}" onclick="openDetailById('${lead.id}')">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="font-medium text-sm truncate">${lead.address || '—'}</span>
                        ${isNew ? '<span class="badge-new">New</span>' : ''}
                    </div>
                    <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">${lead.permit_type || '—'} · ${formatSourceName(lead.source_name)} · ${lead.permit_date ? new Date(lead.permit_date).toLocaleDateString() : '—'}</p>
                </div>
                ${scoreRenderer({value: computeLeadScore(lead), data: lead})}
            </div>
        `;
    }).join('');
}

// ── Hot Leads (Overview Tab — Score 7+) ────────────────────────────────
function renderHotLeads() {
    const container = document.getElementById('hotLeads');
    if (!container) return;

    const hot = [...recentLeads]
        .filter(l => computeLeadScore(l) >= 7)
        .sort((a, b) => computeLeadScore(b) - computeLeadScore(a)
                     || (b.permit_number || '').localeCompare(a.permit_number || ''))
        .slice(0, 20);

    if (hot.length === 0) {
        container.innerHTML = '<p class="text-sm text-gray-500 py-4 text-center">No hot leads yet.</p>';
        return;
    }

    container.innerHTML = hot.map(lead => {
        const isNew = lead.lead_status === 'new';
        return `
            <div class="record-item ${isNew ? 'record-item-new' : ''}" onclick="openDetailById('${lead.id}')">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="font-medium text-sm truncate">${lead.address || '—'}</span>
                        ${isNew ? '<span class="badge-new">New</span>' : ''}
                    </div>
                    <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">${formatSourceName(lead.source_name)} · ${lead.permit_date ? new Date(lead.permit_date).toLocaleDateString() : '—'}</p>
                </div>
                ${scoreRenderer({value: computeLeadScore(lead), data: lead})}
            </div>
        `;
    }).join('');
}

// ── Map View ───────────────────────────────────────────────────────────
function initMap() {
    if (leafletMap) return;
    leafletMap = L.map('mapContainer').setView([25.78, -80.20], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap',
        maxZoom: 19,
    }).addTo(leafletMap);
    updateMapMarkers();
}

function updateMapMarkers() {
    if (!leafletMap) return;
    // Clear existing
    mapMarkers.forEach(m => leafletMap.removeLayer(m));
    mapMarkers = [];

    const showHigh = document.getElementById('mapFilterHigh').checked;
    const showMedium = document.getElementById('mapFilterMedium').checked;
    const showLow = document.getElementById('mapFilterLow').checked;

    let count = 0;
    recentLeads.forEach(lead => {
        const score = computeLeadScore(lead);
        if (score >= 7 && !showHigh) return;
        if (score >= 4 && score < 7 && !showMedium) return;
        if (score < 4 && !showLow) return;

        // Generate lat/lng from address (deterministic pseudo-random for demo)
        const coords = getLeadCoords(lead);
        if (!coords) return;

        const color = score >= 7 ? '#22c55e' : score >= 4 ? '#f59e0b' : '#9ca3af';
        const icon = L.divIcon({
            className: '',
            html: `<div style="width:12px;height:12px;border-radius:50%;background:${color};border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>`,
            iconSize: [12, 12],
            iconAnchor: [6, 6],
        });

        const marker = L.marker(coords, { icon }).addTo(leafletMap);
        marker.bindPopup(`
            <div style="font-family:Inter,sans-serif;min-width:200px">
                <div style="font-weight:600;font-size:13px;margin-bottom:4px">${lead.address || '—'}</div>
                <div style="font-size:11px;color:#6b7280">${lead.permit_type || '—'}</div>
                <div style="font-size:11px;color:#6b7280">${formatSourceName(lead.source_name)} · Score: ${score}</div>
                <div style="margin-top:6px"><a href="#" onclick="openDetailById('${lead.id}');return false;" style="color:#059669;font-size:11px;font-weight:600">View Details →</a></div>
            </div>
        `);
        mapMarkers.push(marker);
        count++;
    });

    document.getElementById('mapLeadCount').textContent = `${count} leads on map`;
}

function getLeadCoords(lead) {
    // Only use real lat/lng from the database — never synthesize fake coordinates.
    // DERM records with Miami-Dade addresses have been geocoded and will appear on the map.
    // Records with null lat/lon (no address or out-of-area address) will not appear.
    if (lead.latitude && lead.longitude) {
        const lat = parseFloat(lead.latitude);
        const lng = parseFloat(lead.longitude);
        // Sanity-check: must be within South Florida bounding box
        if (lat >= 25.1 && lat <= 26.5 && lng >= -81.0 && lng <= -79.9) {
            return [lat, lng];
        }
    }
    return null; // No real coords — don't place on map
}

// ── Historical Data Tab ────────────────────────────────────────────────
function initHistoricalGrid() {
    const columnDefs = [
        {
            field: 'lead_score', headerName: 'Score',
            width: 135, minWidth: 135, maxWidth: 150, suppressSizeToFit: true,
            cellStyle: { textAlign: 'center' },
            valueGetter: (params) => computeLeadScore(params.data),
            cellRenderer: scoreRenderer,
            sort: 'desc', sortIndex: 0,
            comparator: (valA, valB, nodeA, nodeB) => {
                if (valA !== valB) return valA - valB;
                const pnA = (nodeA.data && nodeA.data.permit_number) || '';
                const pnB = (nodeB.data && nodeB.data.permit_number) || '';
                return pnA.localeCompare(pnB);
            },
        },
        {
            field: 'source_name', headerName: 'Source',
            width: 140, minWidth: 110,
            valueFormatter: (p) => formatSourceName(p.value),
        },
        {
            field: 'address', headerName: 'Address',
            minWidth: 150, flex: 1.2,
            cellRenderer: (params) => {
                const addr = params.value || '<em class="text-gray-400">No address</em>';
                return `<span style="cursor:pointer;color:var(--ag-foreground-color)" class="hover:underline" onclick="openDetailById('${params.data.id}')">${addr}</span>`;
            },
        },
        {
            field: 'owner_name', headerName: 'Owner',
            width: 140, minWidth: 110,
            cellStyle: { fontSize: '12px' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'permit_type', headerName: 'Permit Type',
            minWidth: 110, flex: 0.7,
            cellStyle: { fontSize: '12px' },
        },
        {
            field: 'permit_description', headerName: 'Description',
            width: 150, minWidth: 110,
            cellStyle: { fontSize: '11px', color: '#6b7280' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'permit_number', headerName: 'Permit #',
            width: 130, minWidth: 110,
            cellStyle: { fontFamily: 'JetBrains Mono, monospace', fontSize: '11px' },
        },
        {
            field: 'permit_date', headerName: 'Date',
            width: 100, minWidth: 90,
            valueFormatter: (p) => p.value ? new Date(p.value).toLocaleDateString() : '—',
        },
        {
            field: 'permit_status', headerName: 'Permit Status',
            width: 110, minWidth: 90,
            cellStyle: { fontSize: '12px' },
        },
        {
            field: 'jurisdiction', headerName: 'Jurisdiction',
            width: 130, minWidth: 110,
        },
        {
            field: 'contractor_name', headerName: 'Contractor',
            width: 140, minWidth: 110,
            cellStyle: { fontSize: '12px' },
            valueFormatter: (p) => p.value || '—',
        },
        {
            field: 'contractor_phone', headerName: 'Contr. Phone',
            width: 120, minWidth: 100,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="tel:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'owner_phone', headerName: 'Owner Phone',
            width: 120, minWidth: 100,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="tel:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'owner_email', headerName: 'Owner Email',
            width: 160, minWidth: 120,
            cellStyle: { fontSize: '11px' },
            cellRenderer: (p) => p.value ? `<a href="mailto:${p.value}" style="color:#059669;text-decoration:none">${p.value}</a>` : '<span style="color:#d1d5db">—</span>',
        },
        {
            field: 'lead_status', headerName: 'Status',
            width: 100, minWidth: 90,
            cellRenderer: statusRenderer,
        },
        {
            field: 'discovered_at', headerName: 'Discovered',
            width: 160, minWidth: 140,
            valueFormatter: (p) => p.value ? new Date(p.value).toLocaleString() : '—',
        },
    ];

    const gridOptions = {
        columnDefs,
        rowData: [],
        animateRows: true,
        pagination: true,
        paginationPageSize: 100,
        paginationPageSizeSelector: [50, 100, 200, 500],
        defaultColDef: { sortable: true, filter: true, resizable: true },
        onRowDoubleClicked: (e) => openDetail(e.data),
        isExternalFilterPresent: () => !!historicalSourceFilter,
        doesExternalFilterPass: (node) => {
            if (!historicalSourceFilter) return true;
            return node.data && node.data.source_name === historicalSourceFilter;
        },
        onFirstDataRendered: () => _injectScoreInfoBtn(gridDiv),
    };

    const gridDiv = document.getElementById('historicalGrid');
    if (gridDiv) {
        historicalGridApi = agGrid.createGrid(gridDiv, gridOptions);
    }
}

function renderHistoricalData() {
    if (!historicalGridApi) return;
    historicalGridApi.setGridOption('rowData', allLeads);
    renderHistoricalCards();
}

function renderHistoricalCards() {
    const container = document.getElementById('historicalCards');
    if (!container) return;

    const sourceOrder = ['miami_dade_derm', 'fort_lauderdale', 'city_of_miami_tree', 'city_of_miami'];
    const sourceIcons = {
        miami_dade_derm:    '🌿',
        fort_lauderdale:    '🏖️',
        city_of_miami_tree: '🌴',
        city_of_miami:      '🏙️',
    };

    // Count leads per source
    const counts = {};
    let totalCount = allLeads.length;
    allLeads.forEach(l => {
        const src = l.source_name || 'unknown';
        counts[src] = (counts[src] || 0) + 1;
    });

    // "All Sources" card
    const allActive = historicalSourceFilter === '' ? 'hist-card-active' : '';
    let html = `
        <div class="hist-source-card ${allActive}" onclick="filterHistoricalBySource('')">
            <div class="flex items-center gap-3">
                <div class="w-10 h-10 rounded-xl flex items-center justify-center text-xl bg-gray-100 dark:bg-gray-800">📊</div>
                <div>
                    <h4 class="font-bold text-sm">All Sources</h4>
                    <p class="text-xs text-gray-500 dark:text-gray-400">${totalCount.toLocaleString()} total leads</p>
                </div>
            </div>
        </div>
    `;

    sourceOrder.forEach(src => {
        const count = counts[src] || 0;
        const icon = sourceIcons[src] || '📋';
        const active = historicalSourceFilter === src ? 'hist-card-active' : '';
        html += `
            <div class="hist-source-card ${active}" onclick="filterHistoricalBySource('${src}')">
                <div class="flex items-center gap-3">
                    <div class="w-10 h-10 rounded-xl flex items-center justify-center text-xl bg-gray-100 dark:bg-gray-800">${icon}</div>
                    <div>
                        <h4 class="font-bold text-sm">${formatSourceName(src)}</h4>
                        <p class="text-xs text-gray-500 dark:text-gray-400">${count.toLocaleString()} leads</p>
                    </div>
                </div>
            </div>
        `;
    });

    container.innerHTML = html;

    // Update the total badge
    const badge = document.getElementById('historicalCountBadge');
    if (badge) {
        const shown = historicalSourceFilter ? (counts[historicalSourceFilter] || 0) : totalCount;
        badge.textContent = shown.toLocaleString();
    }
}

function filterHistoricalBySource(source) {
    historicalSourceFilter = source;
    if (historicalGridApi) historicalGridApi.onFilterChanged();
    renderHistoricalCards(); // Re-render to update active state
}

// ── Health Tab ─────────────────────────────────────────────────────────
function initHealthGrid() {
    const columnDefs = [
        { field: 'source_name', headerName: 'Source', valueFormatter: p => formatSourceName(p.value), flex: 1 },
        { field: 'job_name', headerName: 'Worker', flex: 1 },
        {
            field: 'status', headerName: 'Status', width: 120,
            cellRenderer: (p) => {
                const s = p.value || 'unknown';
                return `<span class="health-status health-${s}">${s}</span>`;
            },
        },
        { field: 'records_found', headerName: 'Found', width: 90, type: 'numericColumn' },
        { field: 'records_inserted', headerName: 'Inserted', width: 100, type: 'numericColumn' },
        { field: 'records_updated', headerName: 'Updated', width: 100, type: 'numericColumn' },
        {
            field: 'started_at', headerName: 'Started', width: 160,
            valueFormatter: (p) => p.value ? new Date(p.value).toLocaleString() : '—',
        },
        {
            field: 'finished_at', headerName: 'Finished', width: 160,
            valueFormatter: (p) => p.value ? new Date(p.value).toLocaleString() : '—',
        },
        {
            field: 'error_message', headerName: 'Error', flex: 1,
            cellStyle: { color: '#dc2626', fontSize: '12px' },
        },
    ];

    const gridOptions = {
        columnDefs, rowData: [], animateRows: true,
        pagination: true, paginationPageSize: 20,
        defaultColDef: { sortable: true, resizable: true },
    };

    const gridDiv = document.getElementById('healthGrid');
    healthGridApi = agGrid.createGrid(gridDiv, gridOptions);
}

function renderHealthCards(runs) {
    const sources = {};
    const sourceOrder = ['miami_dade_derm', 'fort_lauderdale', 'city_of_miami_tree', 'city_of_miami'];
    const sourceIcons = {
        miami_dade_derm:    '🌿',
        fort_lauderdale:    '🏖️',
        city_of_miami_tree: '🌴',
        city_of_miami:      '🏙️',
    };

    runs.forEach(run => {
        const src = run.source_name;
        if (!sources[src] || new Date(run.started_at) > new Date(sources[src].started_at)) {
            sources[src] = run;
        }
    });

    // Count total leads per source from the loaded data
    const leadCounts = {};
    (recentLeads || []).forEach(l => {
        const s = l.source_name || 'unknown';
        leadCounts[s] = (leadCounts[s] || 0) + 1;
    });

    const container = document.getElementById('healthCards');
    container.innerHTML = sourceOrder.map(src => {
        const run = sources[src];
        const icon = sourceIcons[src] || '📋';
        const totalLeads = leadCounts[src] || 0;

        if (!run) {
            return `
                <div class="health-source-card">
                    <div class="flex items-center gap-3 mb-3">
                        <div class="w-12 h-12 rounded-xl flex items-center justify-center text-2xl bg-gray-100 dark:bg-gray-800">${icon}</div>
                        <div>
                            <h4 class="font-bold text-sm">${formatSourceName(src)}</h4>
                            <p class="text-xs text-gray-500">No runs recorded</p>
                        </div>
                    </div>
                    <div class="space-y-0">
                        <div class="health-row"><span class="health-label">Total Leads</span><span class="health-value">${totalLeads.toLocaleString()}</span></div>
                    </div>
                    <div class="flex justify-end mt-2">
                        <span class="health-status" style="background:#f3f4f6;color:#6b7280">NO DATA</span>
                    </div>
                </div>
            `;
        }

        const statusCls = `health-${run.status}`;
        const errorHtml = run.error_message
            ? `<p class="mt-3 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 p-2 rounded-lg">${run.error_message}</p>`
            : '';

        return `
            <div class="health-source-card">
                <div class="flex items-center gap-3 mb-3">
                    <div class="w-12 h-12 rounded-xl flex items-center justify-center text-2xl bg-gray-100 dark:bg-gray-800">${icon}</div>
                    <div class="flex-1 min-w-0">
                        <h4 class="font-bold text-sm">${formatSourceName(src)}</h4>
                        <p class="text-xs text-gray-500 dark:text-gray-400">${run.finished_at ? new Date(run.finished_at).toLocaleString() : 'In progress...'}</p>
                    </div>
                    <span class="health-status ${statusCls}">${(run.status || 'unknown').toUpperCase()}</span>
                </div>
                <div class="space-y-0">
                    <div class="health-row"><span class="health-label">Total Leads</span><span class="health-value font-semibold">${totalLeads.toLocaleString()}</span></div>
                    <div class="health-row"><span class="health-label">New Records</span><span class="health-value">${(run.records_found || 0).toLocaleString()}</span></div>
                    <div class="health-row"><span class="health-label">Inserted</span><span class="health-value">${(run.records_inserted || 0).toLocaleString()}</span></div>
                    <div class="health-row"><span class="health-label">Updated</span><span class="health-value">${(run.records_updated || 0).toLocaleString()}</span></div>
                </div>
                ${errorHtml}
            </div>
        `;
    }).join('');
}

// ── Email Modal ────────────────────────────────────────────────────────
const EMAIL_SUBSCRIBE_COOLDOWN_MS = 5 * 60 * 1000; // 5 minutes — matches Worker debounce
let lastSubscribeAt = 0;

function openEmailModal() {
    if (!requireAuth('manage email settings')) return;
    document.getElementById('emailModal').classList.remove('hidden');
    // Pre-fill from localStorage if previously saved
    const saved = JSON.parse(localStorage.getItem('emailPrefs') || '{}');
    if (saved.email) document.getElementById('emailAddress').value = saved.email;
    if (saved.daily !== undefined) document.getElementById('emailDigest').checked = saved.daily;
    if (saved.newLeads !== undefined) document.getElementById('emailNewLeads').checked = saved.newLeads;
    if (saved.errors !== undefined) document.getElementById('emailErrors').checked = saved.errors;
}

function closeEmailModal() {
    document.getElementById('emailModal').classList.add('hidden');
}

async function saveEmailSettings() {
    if (!requireAuth('save email settings')) return;
    const email = document.getElementById('emailAddress').value.trim();
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        showToast('Please enter a valid email address', 'error');
        return;
    }

    // Client-side debounce — mirror the Worker's 5-min debounce to avoid accidental double-dispatch
    const now = Date.now();
    if (now - lastSubscribeAt < EMAIL_SUBSCRIBE_COOLDOWN_MS && lastSubscribeAt > 0) {
        const remaining = Math.ceil((EMAIL_SUBSCRIBE_COOLDOWN_MS - (now - lastSubscribeAt)) / 60000);
        showToast(`Already subscribed recently. Try again in ~${remaining} min.`, 'error');
        return;
    }
    lastSubscribeAt = now;

    const prefs = {
        daily: document.getElementById('emailDigest').checked,
        newLeads: document.getElementById('emailNewLeads').checked,
        errors: document.getElementById('emailErrors').checked,
    };

    // Save to localStorage as cache
    localStorage.setItem('emailPrefs', JSON.stringify({ email, ...prefs }));

    // POST to Cloudflare Worker — it upserts Supabase AND dispatches the adhoc report
    const workerUrl = CONFIG.WORKER_URL;
    if (!workerUrl.includes('YOUR_SUBDOMAIN')) {
        try {
            const resp = await fetch(`${workerUrl}/api/subscribe`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    email,
                    daily_digest: prefs.daily,
                    new_lead_alerts: prefs.newLeads,
                    error_alerts: prefs.errors,
                }),
            });
            if (!resp.ok) throw new Error(`Worker returned ${resp.status}`);
            closeEmailModal();
            showToast('✅ Subscribed! A welcome report is on its way.');
        } catch (err) {
            console.warn('Worker subscribe failed, falling back to direct Supabase save:', err);
            // Fallback: save directly to Supabase without adhoc report
            if (supabaseClient) {
                await supabaseClient.from('email_subscribers').upsert({
                    email,
                    daily_digest: prefs.daily,
                    new_lead_alerts: prefs.newLeads,
                    error_alerts: prefs.errors,
                    subscribed_at: new Date().toISOString(),
                    is_active: true,
                }, { onConflict: 'email' });
            }
            closeEmailModal();
            showToast('Settings saved. Report will arrive with the next daily digest.', 'info');
        }
    } else {
        // WORKER_URL not yet configured — save direct to Supabase
        if (supabaseClient) {
            try {
                const { error } = await supabaseClient
                    .from('email_subscribers')
                    .upsert({
                        email,
                        daily_digest: prefs.daily,
                        new_lead_alerts: prefs.newLeads,
                        error_alerts: prefs.errors,
                        subscribed_at: new Date().toISOString(),
                        is_active: true,
                    }, { onConflict: 'email' });
                if (error) throw error;
                closeEmailModal();
                showToast('✅ Subscribed! Report will arrive with the next daily digest.');
            } catch (err) {
                console.error('Subscription save failed:', err);
                closeEmailModal();
                showToast('Settings saved locally. Will sync when online.', 'info');
            }
        } else {
            closeEmailModal();
            showToast('Email settings saved locally.', 'info');
        }
    }
}

// ── Toast ──────────────────────────────────────────────────────────────
function showToast(message, type = 'info') {
    const toast = document.getElementById('toastNotification');
    if (!toast) return;

    const bgClass = type === 'error' 
        ? 'bg-red-600 text-white' 
        : isDark() ? 'bg-gray-100 text-gray-900' : 'bg-gray-900 text-white';

    toast.innerHTML = `<div class="px-4 py-2.5 rounded-xl shadow-xl text-sm font-medium ${bgClass} toast-enter">${message}</div>`;
    toast.classList.remove('hidden');

    setTimeout(() => {
        const inner = toast.querySelector('div');
        if (inner) inner.classList.add('toast-exit');
        setTimeout(() => {
            toast.classList.add('hidden');
            toast.innerHTML = '';
        }, 300);
    }, 2500);
}

// ── Demo Data ──────────────────────────────────────────────────────────
function loadDemoData() {
    const jurisdictions = ['Miami-Dade County', 'Fort Lauderdale', 'City of Miami'];
    const sources = ['Miami-Dade DERM', 'Fort Lauderdale', 'City of Miami'];
    const permitTypes = [
        'Landscape Tree Removal-Relocation Permit',
        'TREE REMOVAL',
        'TREE PERMIT',
        'VEGETATION REMOVAL',
        'ARBOR PERMIT',
        'Tree Trimming Permit',
        'Mangrove Trimming Permit',
    ];
    const statuses = ['new', 'new', 'new', 'new', 'approved', 'rejected', 'exported'];
    const addresses = [
        '1200 NW 7 AVE', '4521 SW 34 ST', '99 SE 14 ST', '330 SW 16 ST',
        '777 BRICKELL AVE', '2850 TIGERTAIL AVE', '1463 NE 63 CT', '215 SW 10 ST',
        '5727 N FEDERAL HWY', '850 MERIDIAN AVE', '431 SW 56 AV', '1350 NW 50 ST',
        '2881 SW 33 CT', '204 NE 17 AVE', '332 SW 16 ST', '6340 NW 21 AVE',
        '15120 SW 159 CT', '14207 KENDALE LAKES CIR', '9555 SW 162 AVE', '73 W FLAGLER ST',
        '700 SW 57 AVE', '470 NW 83 ST', '1921 N BAYSHORE DR', '345 OCEAN DR',
        '8800 SW 152 ST', '3001 GRAND AVE', '100 S BISCAYNE BLVD', '2655 COLLINS AVE',
        '4000 ALTON RD', '1111 LINCOLN RD', '555 WASHINGTON AVE', '900 S MIAMI AVE',
        '250 CATALONIA AVE', '1500 BAY RD', '6901 COLLINS AVE', '3400 SW 27 AVE',
        '7700 N KENDALL DR', '18501 PINES BLVD', '2000 CONVENTION CENTER DR', '401 BISCAYNE BLVD',
    ];
    const contractors = [
        'ABC Tree Service LLC', 'South Florida Tree Pros', 'Green Canopy Removal',
        'TrueTrimmer Corp', 'Evergreen Tree Solutions', 'Tropical Arborists Inc',
        'Palm Beach Tree Masters', null, null, null,
    ];
    const descriptions = [
        'Tree removal - Large Oak in backyard, dead/hazardous',
        'Removal of 3 palm trees for new construction',
        'Vegetation clearing for commercial development',
        'Emergency tree removal - Hurricane damage',
        'Tree trimming and crown reduction - Banyan',
        'Mangrove trimming along waterfront property',
        'Tree relocation for parking lot expansion',
        'Removal of invasive species - Melaleuca trees',
    ];

    allLeads = [];
    for (let i = 0; i < 75; i++) {
        const srcIdx = i % sources.length;
        const jIdx = srcIdx === 0 ? 0 : srcIdx === 1 ? 1 : 2;
        const daysAgo = Math.floor(Math.random() * 30);
        const date = new Date();
        date.setDate(date.getDate() - daysAgo);
        const contractor = contractors[Math.floor(Math.random() * contractors.length)];
        const score = Math.floor(Math.random() * 9) + 1;

        allLeads.push({
            id: `demo-${i}`,
            source_name: sources[srcIdx],
            jurisdiction: jurisdictions[jIdx],
            address: addresses[i % addresses.length],
            permit_type: permitTypes[i % permitTypes.length],
            permit_description: descriptions[i % descriptions.length],
            permit_number: `TREE-${String(25000 + i).padStart(8, '0')}`,
            permit_status: 'Issued',
            permit_date: date.toISOString().slice(0, 10),
            owner_name: `Property Owner ${i + 1}`,
            contractor_name: contractor,
            contractor_phone: contractor ? `(${305 + Math.floor(Math.random() * 50)}) ${String(Math.floor(Math.random() * 9000000) + 1000000)}` : null,
            source_url: '#',
            lead_score: score,
            lead_status: statuses[Math.floor(Math.random() * statuses.length)],
            lead_type: 'permit',
            discovered_at: new Date(Date.now() - daysAgo * 86400000).toISOString(),
            raw_payload_json: '{}',
        });
    }

    // In demo mode, recentLeads = allLeads (all demo items are within 30 days anyway)
    recentLeads = allLeads;
    onLeadsLoaded();
    showToast('Loaded demo data (Supabase not configured)');
}

// ═══════════════════════════════════════════════════════════════════════
//  SCORING RULES — Load, Save, UI Controls
// ═══════════════════════════════════════════════════════════════════════

/** Load scoring rules from the scoring_rules singleton row in Supabase */
async function loadScoringRules() {
    if (!supabaseClient) return;
    try {
        const { data, error } = await supabaseClient
            .from('scoring_rules')
            .select('*')
            .eq('id', 1)
            .single();
        if (error) throw error;
        if (data) {
            for (const key of Object.keys(SCORING_DEFAULTS)) {
                if (data[key] != null) scoringRules[key] = Number(data[key]);
            }
        }
        console.log('Scoring rules loaded from DB');
    } catch (err) {
        console.warn('Could not load scoring rules — using defaults:', err.message || err);
    }
}

/** Populate every stepper / input in the Scoring Rules tab from scoringRules */
function populateScoringRulesUI() {
    const r = scoringRules;
    for (const key of Object.keys(SCORING_DEFAULTS)) {
        const el = document.getElementById('val_' + key);
        if (!el) continue;
        const meta = SCORING_META[key];
        if (el.tagName === 'INPUT') {
            el.value = meta && meta[3] ? r[key].toFixed(2) : r[key];
        } else {
            el.textContent = meta && meta[3] ? r[key].toFixed(2) : r[key];
        }
    }
    const lbl = document.getElementById('lbl_parcel_acres');
    if (lbl) lbl.textContent = r.parcel_acres_threshold.toFixed(2);

    renderScoringLegendPreview();
    scoringRulesDirty = false;
    _updateSaveBtn(false);
}

/** Build the live legend preview at the top of the scoring tab */
function renderScoringLegendPreview() {
    const r = scoringRules;
    const el = document.getElementById('scoringLegendPreview');
    if (!el) return;

    // Calculate theoretical max score
    const maxGeneral = r.tree_removal_bonus + r.recency_tier1_bonus + r.large_parcel_bonus + r.right_of_way_bonus;
    const maxDerm  = r.tree_removal_bonus + Math.max(r.derm_tier1_bonus, r.derm_tier2_bonus, r.derm_tier3_bonus) + r.large_parcel_bonus + r.right_of_way_bonus;
    const maxScore = Math.max(maxGeneral, maxDerm);
    const badgeEl = document.getElementById('legendMaxScore');
    if (badgeEl) badgeEl.textContent = `Max ${maxScore} pts`;

    el.innerHTML = `
        <div class="legend-group">
            <div class="legend-group-title title-bonus">Bonuses</div>
            <div class="legend-item"><span class="legend-chip legend-chip-bonus">+${r.tree_removal_bonus}</span> <span class="legend-label">Tree removal / arbor</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-bonus">+${r.vegetation_removal_bonus}</span> <span class="legend-label">Vegetation removal</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-bonus">+${r.landscape_installation_bonus}</span> <span class="legend-label">Landscape / ROW</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-bonus">+${r.large_parcel_bonus}</span> <span class="legend-label">Parcel &gt; ${r.parcel_acres_threshold.toFixed(2)} ac</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-bonus">+${r.right_of_way_bonus}</span> <span class="legend-label">Right-of-way</span></div>
        </div>
        <div class="legend-group">
            <div class="legend-group-title title-recency">General Recency</div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.recency_tier1_bonus}</span> <span class="legend-label">Hot · 0–${r.recency_tier1_days_max}d</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.recency_tier2_bonus}</span> <span class="legend-label">Warm · ${r.recency_tier1_days_max + 1}–${r.recency_tier2_days_max}d</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.recency_tier3_bonus}</span> <span class="legend-label">Aging · ${r.recency_tier2_days_max + 1}–${r.recency_tier3_days_max}d</span></div>
        </div>
        <div class="legend-group">
            <div class="legend-group-title title-derm">DERM Recency</div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.derm_tier1_bonus}</span> <span class="legend-label">T1 · ${r.derm_tier1_days_min}–${r.derm_tier1_days_max}d</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.derm_tier2_bonus}</span> <span class="legend-label">T2 · ${r.derm_tier2_days_min}–${r.derm_tier2_days_max}d</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-tier">+${r.derm_tier3_bonus}</span> <span class="legend-label">T3 · ${r.derm_tier3_days_min}–${r.derm_tier3_days_max}d</span></div>
        </div>
        <div class="legend-group">
            <div class="legend-group-title title-penalty">Penalties</div>
            <div class="legend-item"><span class="legend-chip legend-chip-penalty">−${r.contractor_penalty}</span> <span class="legend-label">Contractor assigned</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-penalty">−${r.derm_no_address_penalty}</span> <span class="legend-label">DERM no address</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-penalty">−${r.intended_decision_penalty}</span> <span class="legend-label">Intended Decision</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-penalty">−${r.corrections_required_penalty}</span> <span class="legend-label">Corrections Req'd</span></div>
            <div class="legend-item"><span class="legend-chip legend-chip-penalty">−${r.staleness_penalty}</span> <span class="legend-label">Stale (&gt; ${r.staleness_days_threshold}d)</span></div>
        </div>
    `;
}

/** Toggle legend preview collapse/expand */
function toggleLegendPreview() {
    const body = document.getElementById('scoringLegendBody');
    const chevron = document.getElementById('legendChevron');
    const btn = body?.closest('.scoring-legend-card')?.querySelector('.scoring-legend-header');
    if (!body || !chevron) return;
    const isCollapsed = body.classList.toggle('collapsed');
    chevron.classList.toggle('collapsed', isCollapsed);
    if (btn) btn.setAttribute('aria-expanded', !isCollapsed);
}

/** Debounced live preview — refresh grids + charts when scoring rules change */
let _scoringPreviewTimer = null;
function _debouncedScoringPreview() {
    if (_scoringPreviewTimer) clearTimeout(_scoringPreviewTimer);
    _scoringPreviewTimer = setTimeout(() => {
        // Re-compute scores in the grids (immediate visual feedback)
        if (leadGridApi) leadGridApi.refreshCells({ columns: ['lead_score'], force: true });
        if (historicalGridApi) historicalGridApi.refreshCells({ columns: ['lead_score'], force: true });
        // Re-render overview charts that depend on scores
        try { renderScoresChart(); } catch(e) {}
        try { renderOverviewStats(); } catch(e) {}
    }, 300);
}

/** Step a rule value by +/- delta, clamped to its defined range */
function stepRule(key, delta) {
    const meta = SCORING_META[key];
    if (!meta) return;
    const [min, max, step, isDecimal] = meta;
    let val = scoringRules[key] + delta * step;
    val = Math.max(min, Math.min(max, isDecimal ? Math.round(val * 100) / 100 : Math.round(val)));
    scoringRules[key] = val;

    const el = document.getElementById('val_' + key);
    if (el) {
        const display = isDecimal ? val.toFixed(2) : val;
        if (el.tagName === 'INPUT') el.value = display;
        else el.textContent = display;
    }

    if (key === 'parcel_acres_threshold') {
        const lbl = document.getElementById('lbl_parcel_acres');
        if (lbl) lbl.textContent = val.toFixed(2);
    }

    scoringRulesDirty = true;
    _updateSaveBtn(true);
    renderScoringLegendPreview();
    _debouncedScoringPreview();
}

/** Handle manual input change (for DERM tier day range inputs) */
function onRuleInputChange(key, inputEl) {
    const meta = SCORING_META[key];
    if (!meta) return;
    const [min, max, , isDecimal] = meta;
    let val = isDecimal ? parseFloat(inputEl.value) : parseInt(inputEl.value, 10);
    if (isNaN(val)) val = SCORING_DEFAULTS[key];
    val = Math.max(min, Math.min(max, val));
    scoringRules[key] = val;
    inputEl.value = isDecimal ? val.toFixed(2) : val;

    scoringRulesDirty = true;
    _updateSaveBtn(true);
    renderScoringLegendPreview();
    _debouncedScoringPreview();
}

/** Visual indicator on the save button */
function _updateSaveBtn(dirty) {
    const btn = document.getElementById('saveScoringBtn');
    if (!btn) return;
    if (dirty) {
        btn.innerHTML = `
            <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
            Save Rules *`;
        btn.classList.remove('saved');
    } else {
        btn.innerHTML = `
            <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
            Save Rules`;
        btn.classList.remove('saved');
    }
}

/** Save current scoring rules to Supabase and refresh everything */
async function saveScoringRules() {
    if (!supabaseClient) {
        showToast('Cannot save — Supabase not connected', 'error');
        return;
    }
    const btn = document.getElementById('saveScoringBtn');
    if (btn) btn.classList.add('saving');

    try {
        const payload = {};
        for (const key of Object.keys(SCORING_DEFAULTS)) {
            payload[key] = scoringRules[key];
        }

        const { error } = await supabaseClient
            .from('scoring_rules')
            .update(payload)
            .eq('id', 1);
        if (error) throw error;

        scoringRulesDirty = false;
        _updateSaveBtn(false);
        _rebuildScorePopover();

        if (leadGridApi) leadGridApi.refreshCells({ columns: ['lead_score'], force: true });
        if (historicalGridApi) historicalGridApi.refreshCells({ columns: ['lead_score'], force: true });

        try { renderOverviewStats(); } catch(e) {}
        try { renderScoresChart(); } catch(e) {}

        if (btn) {
            btn.classList.remove('saving');
            btn.classList.add('saved');
            btn.innerHTML = `
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
                Saved ✓`;
            setTimeout(() => _updateSaveBtn(false), 2000);
        }
        showToast('Scoring rules saved! Existing leads will be re-scored on next pipeline run.', 'success');
    } catch (err) {
        console.error('Failed to save scoring rules:', err);
        if (btn) btn.classList.remove('saving');
        showToast('Failed to save scoring rules: ' + (err.message || err), 'error');
    }
}

/** Reset all rules to recommended defaults */
async function resetScoringRulesToDefaults() {
    if (!confirm('Reset all scoring rules to their recommended default values?')) return;
    scoringRules = { ...SCORING_DEFAULTS };
    populateScoringRulesUI();
    scoringRulesDirty = true;
    _updateSaveBtn(true);
    showToast('Rules reset to defaults — click "Save Rules" to persist.', 'info');
}

function loadDemoHealthData() {
    const demoRuns = [
        {
            id: 'demo-run-1', job_name: 'derm_tree_worker', source_name: 'miami_dade_derm',
            started_at: new Date(Date.now() - 3600000).toISOString(),
            finished_at: new Date(Date.now() - 3500000).toISOString(),
            status: 'success', records_found: 128, records_inserted: 12, records_updated: 116, error_message: null,
        },
        {
            id: 'demo-run-2', job_name: 'fort_lauderdale_worker', source_name: 'fort_lauderdale',
            started_at: new Date(Date.now() - 3000000).toISOString(),
            finished_at: new Date(Date.now() - 2900000).toISOString(),
            status: 'success', records_found: 45, records_inserted: 8, records_updated: 37, error_message: null,
        },
        {
            id: 'demo-run-3', job_name: 'miami_tree_worker', source_name: 'city_of_miami_tree',
            started_at: new Date(Date.now() - 2400000).toISOString(),
            finished_at: new Date(Date.now() - 2300000).toISOString(),
            status: 'success', records_found: 22, records_inserted: 5, records_updated: 17, error_message: null,
        },
        {
            id: 'demo-run-4', job_name: 'miami_building_worker', source_name: 'city_of_miami',
            started_at: new Date(Date.now() - 1800000).toISOString(),
            finished_at: new Date(Date.now() - 1700000).toISOString(),
            status: 'success', records_found: 340, records_inserted: 3, records_updated: 337, error_message: null,
        },
    ];

    allJobRuns = demoRuns;
    if (healthGridApi) healthGridApi.setGridOption('rowData', demoRuns);
    renderHealthCards(demoRuns);
}
