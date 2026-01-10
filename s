<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1.0" />
  <title>Candidate Role Matcher | Hexona (Demo)</title>

  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">

  <style>
    :root{
      --bg-primary:#0f1419;
      --bg-secondary:#1a1f2e;
      --bg-card:#1e2433;
      --bg-hover:#252b3b;
      --border:#2d3548;
      --border-light:#3a4355;

      --text-primary:#ffffff;
      --text-secondary:#b4bcd0;
      --text-muted:#7d8590;

      --accent:#0078d4;
      --accent-hover:#106ebe;
      --accent-light:#4da3ff;

      --success:#0f7b0f;
      --success-light:#13a10e;

      --warning:#f7b500;
      --error:#e74856;

      --purple:#7c3aed;
      --purple-hover:#6d28d9;
      --purple-light:#a78bfa;

      --radius-sm:4px;
      --radius-md:6px;
      --radius-lg:8px;

      --shadow:0 1px 3px rgba(0,0,0,.2);
      --shadow-md:0 2px 10px rgba(0,0,0,.35);
    }

    *{box-sizing:border-box;margin:0;padding:0}

    body{
      font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      background:var(--bg-primary);
      color:var(--text-primary);
      min-height:100vh;
      line-height:1.5;
      font-size:14px;
      -webkit-font-smoothing:antialiased;
    }

    .demo-badge{
      position:fixed;
      top:16px;
      right:16px;
      padding:6px 12px;
      background:var(--accent);
      border-radius:var(--radius-sm);
      font-size:11px;
      font-weight:600;
      letter-spacing:0.5px;
      text-transform:uppercase;
      z-index:10000;
    }

    .container{
      max-width:1200px;
      margin:0 auto;
      padding:24px 24px 70px;
    }

    .header{
      margin-bottom:24px;
      padding-bottom:16px;
      border-bottom:1px solid var(--border);
    }
    .header h1{
      font-size:24px;
      font-weight:600;
      margin-bottom:4px;
      letter-spacing:-0.3px;
    }
    .header .subtitle{
      font-size:13px;
      color:var(--text-muted);
      font-weight:400;
    }

    .grid{
      display:grid;
      grid-template-columns:1fr;
      gap:16px;
    }

    .card{
      background:var(--bg-card);
      border:1px solid var(--border);
      border-radius:var(--radius-lg);
      padding:20px;
      box-shadow:var(--shadow);
    }

    .card-title{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      margin-bottom:16px;
      padding-bottom:14px;
      border-bottom:1px solid var(--border);
    }
    .card-title h2{
      font-size:16px;
      font-weight:600;
      color:var(--text-primary);
    }
    .card-title .meta{
      font-size:11px;
      color:var(--text-muted);
      padding:4px 8px;
      background:var(--bg-secondary);
      border-radius:var(--radius-sm);
      font-weight:500;
      text-transform:uppercase;
      letter-spacing:0.5px;
    }

    label{
      display:block;
      font-size:12px;
      font-weight:600;
      color:var(--text-secondary);
      margin-bottom:6px;
    }

    input[type="text"], input[type="number"], input[type="file"], textarea{
      width:100%;
      padding:8px 12px;
      border-radius:var(--radius-sm);
      border:1px solid var(--border);
      background:var(--bg-secondary);
      color:var(--text-primary);
      font-size:13px;
      font-weight:400;
      font-family:inherit;
      transition:all .2s ease;
    }
    input[type="file"]{padding:9px 12px; cursor:pointer;}
    textarea{resize:vertical; min-height:86px; line-height:1.5;}
    input:hover, textarea:hover{border-color:var(--border-light);}
    input:focus, textarea:focus{
      outline:none;
      border-color:var(--accent);
      box-shadow:0 0 0 1px var(--accent);
    }

    input[readonly]{
      opacity:.9;
      color:var(--text-muted);
      -webkit-text-fill-color:var(--text-muted);
      background:rgba(26,31,46,.75);
      cursor:not-allowed;
    }

    .hint{
      font-size:12px;
      color:var(--text-muted);
      margin-top:6px;
    }

    .error{
      display:none;
      margin-top:10px;
      padding:10px 12px;
      border-radius:var(--radius-md);
      background:rgba(231,72,86,.12);
      border:1px solid rgba(231,72,86,.35);
      color:var(--error);
      font-size:13px;
      font-weight:600;
    }

    .form-grid{
      display:grid;
      grid-template-columns:1fr;
      gap:14px;
      margin-bottom:14px;
    }

    .two-col{
      display:grid;
      grid-template-columns:1fr;
      gap:12px;
    }
    @media(min-width:860px){
      .two-col{grid-template-columns:1fr 1fr;}
    }

    .inline{
      display:flex;
      gap:10px;
      align-items:flex-end;
      flex-wrap:wrap;
    }
    .inline .grow{flex:1; min-width:260px;}
    .inline .tight{min-width:220px;}

    .checkbox{
      display:flex;
      align-items:center;
      gap:10px;
      padding:10px 12px;
      background:var(--bg-secondary);
      border:1px solid var(--border);
      border-radius:var(--radius-sm);
      cursor:pointer;
      transition:all .2s ease;
      user-select:none;
      white-space:nowrap;
    }
    .checkbox:hover{border-color:var(--border-light); background:var(--bg-hover);}
    .checkbox input{
      width:16px;height:16px;
      cursor:pointer;
      accent-color:var(--accent);
    }
    .checkbox .label{
      font-weight:600;
      font-size:12px;
      color:var(--text-secondary);
      margin:0;
    }

    .segmented{
      display:flex;
      border:1px solid var(--border);
      border-radius:var(--radius-md);
      background:var(--bg-secondary);
      overflow:hidden;
    }
    .segmented button{
      flex:1;
      padding:10px 12px;
      border:none;
      background:transparent;
      color:var(--text-secondary);
      font-weight:600;
      font-size:12px;
      cursor:pointer;
      transition:all .2s ease;
    }
    .segmented button:hover{background:var(--bg-hover);}
    .segmented button.active{
      background:rgba(0,120,212,.12);
      color:var(--accent-light);
      box-shadow:inset 0 0 0 1px rgba(0,120,212,.35);
    }

    .btn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:8px;
      padding:10px 14px;
      border-radius:var(--radius-sm);
      border:1px solid var(--border);
      background:var(--bg-secondary);
      color:var(--text-primary);
      font-weight:600;
      font-size:13px;
      cursor:pointer;
      transition:all .2s ease;
      font-family:inherit;
      text-decoration:none;
    }
    .btn:hover:not(:disabled){background:var(--bg-hover); border-color:var(--border-light);}
    .btn:active:not(:disabled){transform:scale(.99);}
    .btn:disabled{opacity:.55; cursor:not-allowed;}

    .btn.primary{
      background:var(--accent);
      border-color:var(--accent);
      color:#fff;
    }
    .btn.primary:hover:not(:disabled){background:var(--accent-hover); border-color:var(--accent-hover);}

    .btn.success{
      background:var(--success);
      border-color:var(--success);
      color:#fff;
    }
    .btn.success:hover:not(:disabled){background:var(--success-light); border-color:var(--success-light);}

    .btn.purple{
      background:var(--purple);
      border-color:var(--purple);
      color:#fff;
    }
    .btn.purple:hover:not(:disabled){background:var(--purple-hover); border-color:var(--purple-hover);}

    .btn.ghost{
      background:transparent;
      border-color:var(--border);
    }

    .btn.small{padding:7px 10px; font-size:12px;}

    .processing{
      display:none;
      margin-top:14px;
      padding:12px;
      background:var(--bg-secondary);
      border:1px solid var(--border);
      border-radius:var(--radius-md);
      align-items:center;
      gap:12px;
    }
    .spinner{
      width:18px;height:18px;
      border:2px solid rgba(0,120,212,.25);
      border-top-color:var(--accent);
      border-radius:50%;
      animation:spin .8s linear infinite;
    }
    @keyframes spin{to{transform:rotate(360deg)}}

    .results{
      display:none;
      margin-top:16px;
    }

    .results-summary{
      display:flex;
      align-items:center;
      justify-content:space-between;
      flex-wrap:wrap;
      gap:12px;
      margin-bottom:12px;
    }
    .results-summary .text{
      color:var(--text-secondary);
      font-size:13px;
      font-weight:600;
    }
    .jump-buttons{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
    }

    .section{
      margin-top:14px;
      border:1px solid var(--border);
      border-radius:var(--radius-lg);
      overflow:hidden;
      background:var(--bg-secondary);
    }
    .section-head{
      padding:12px 14px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      border-bottom:1px solid var(--border);
      background:rgba(30,36,51,.6);
    }
    .section-head h3{
      font-size:14px;
      font-weight:700;
      letter-spacing:-0.2px;
    }
    .section-head .badge{
      padding:4px 10px;
      border-radius:999px;
      font-size:12px;
      font-weight:700;
      border:1px solid var(--border);
      background:var(--bg-card);
      color:var(--text-secondary);
    }

    .section.active .section-head h3{ color:var(--success-light); }
    .section.passive .section-head h3{ color:var(--purple-light); }

    .candidate{
      padding:14px;
      border-top:1px solid var(--border);
      background:transparent;
    }
    .candidate:first-child{border-top:none;}

    .cand-top{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap:12px;
      margin-bottom:10px;
      flex-wrap:wrap;
    }
    .cand-name{
      font-size:14px;
      font-weight:700;
      letter-spacing:-0.1px;
      display:flex;
      align-items:center;
      gap:8px;
      flex-wrap:wrap;
    }
    .pill{
      display:inline-flex;
      align-items:center;
      gap:6px;
      padding:3px 10px;
      border-radius:999px;
      border:1px solid var(--border);
      background:var(--bg-card);
      color:var(--text-secondary);
      font-size:12px;
      font-weight:700;
      white-space:nowrap;
    }
    .pill.active{
      border-color:rgba(15,123,15,.35);
      background:rgba(15,123,15,.12);
      color:var(--success-light);
    }
    .pill.passive{
      border-color:rgba(124,58,237,.35);
      background:rgba(124,58,237,.12);
      color:var(--purple-light);
    }
    .pill.shortlisted{
      border-color:rgba(247,181,0,.35);
      background:rgba(247,181,0,.12);
      color:var(--warning);
    }

    .cand-meta{
      color:var(--text-muted);
      font-size:12px;
      font-weight:600;
      margin-top:2px;
    }

    .cand-actions{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      justify-content:flex-end;
    }

    .grid-mini{
      display:grid;
      grid-template-columns:1fr;
      gap:10px;
      margin-top:10px;
    }
    @media(min-width:900px){
      .grid-mini{grid-template-columns:1fr 1fr;}
    }

    .block{
      border:1px solid var(--border);
      background:rgba(30,36,51,.55);
      border-radius:var(--radius-md);
      padding:10px 12px;
    }
    .block h4{
      font-size:12px;
      font-weight:800;
      margin-bottom:6px;
      letter-spacing:0.3px;
      text-transform:uppercase;
      color:var(--text-muted);
    }

    /* Requested: Experience match heading golden like before */
    .block h4.exp{ color:var(--warning); }

    .bullets{
      margin:0;
      padding-left:18px;
      color:var(--text-secondary);
      font-size:13px;
    }
    .bullets li{ margin:3px 0; }

    .download-row{
      margin-top:12px;
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      justify-content:flex-end;
    }

    /* Modal */
    .overlay{
      position:fixed;
      inset:0;
      background:rgba(0,0,0,.75);
      backdrop-filter:blur(4px);
      display:none;
      align-items:center;
      justify-content:center;
      z-index:9999;
      padding:20px;
    }
    .overlay.show{display:flex;}
    .modal{
      width:min(980px,100%);
      max-height:92vh;
      overflow:auto;
      background:var(--bg-card);
      border:1px solid var(--border);
      border-radius:var(--radius-lg);
      box-shadow:var(--shadow-md);
    }
    .modal-head{
      padding:14px 16px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      border-bottom:1px solid var(--border);
      background:var(--bg-secondary);
      gap:12px;
    }
    .modal-head h3{
      font-size:15px;
      font-weight:700;
      letter-spacing:-0.2px;
    }
    .modal-body{padding:16px;}
    .modal-actions{
      padding:14px 16px;
      border-top:1px solid var(--border);
      background:var(--bg-secondary);
      display:flex;
      gap:10px;
      justify-content:flex-end;
      flex-wrap:wrap;
    }

    .modal-grid{
      display:grid;
      grid-template-columns:1fr;
      gap:12px;
    }

    .email-textarea{
      min-height:380px; /* requested: full email visible without having to drag-resize */
      resize:none;
      white-space:pre-wrap;
    }

    /* Toast */
    .toast{
      position:fixed;
      left:50%;
      bottom:22px;
      transform:translateX(-50%);
      background:rgba(26,31,46,.95);
      border:1px solid var(--border);
      border-radius:999px;
      padding:10px 14px;
      color:var(--text-secondary);
      font-size:13px;
      font-weight:600;
      box-shadow:var(--shadow-md);
      display:none;
      z-index:10001;
      max-width:min(820px, calc(100% - 30px));
      text-align:center;
    }
    .toast.show{display:block;}

    @media(max-width:768px){
      .container{padding:16px 16px 60px;}
      .card{padding:16px;}
      .email-textarea{min-height:320px;}
      .cand-actions .btn{width:100%;}
      .jump-buttons .btn{width:100%;}
    }
  </style>
</head>

<body>
  <div class="demo-badge">Demo</div>
  <div class="toast" id="toast"></div>

  <div class="container">
    <div class="header">
      <h1>Candidate Role Matcher</h1>
      <div class="subtitle".</div>
    </div>

    <div class="grid">
      <!-- Setup -->
      <div class="card">
        <div class="card-title">
          <h2>Match setup</h2>
          <div class="meta">Demo</div>
        </div>

        <div class="form-grid">
          <div>
            <label for="jdFile">Job description (upload)</label>
            <input type="file" id="jdFile" accept=".txt,.pdf,.doc,.docx" />
          </div>

          <div class="inline">
            <div class="grow">
              <label for="jobId">Job ID</label>
              <input type="text" id="jobId" placeholder="e.g. JOB-2025-001" />
            </div>
            <label class="checkbox">
              <input type="checkbox" id="noJobId" />
              <span class="label">No job ID</span>
            </label>
          </div>

          <div>
            <label>Candidate pool scope</label>
            <div class="segmented" role="tablist" aria-label="Candidate pool scope">
              <button type="button" id="scopeActive" class="active" aria-selected="true">Prefer active (still shows passive)</button>
              <button type="button" id="scopeInclude" aria-selected="false">Include wider passive pool</button>
            </div>
            <div class="hint">Demo always includes at least 2 passive candidates so you can see the outreach workflow.</div>
          </div>

          <div class="two-col">
            <div>
              <label for="town">Job listing town / city</label>
              <input type="text" id="town" placeholder="e.g. Newcastle upon Tyne" />
            </div>
            <div>
              <label for="radius">Candidate distance radius (km)</label>
              <input type="number" id="radius" value="50" min="1" />
            </div>
          </div>

          <div class="two-col">
            <div>
              <label for="count">Number of candidates to return</label>
              <input type="number" id="count" value="5" min="5" />
              <div id="countError" class="error" role="alert">Please select at least 5 candidates.</div>
            </div>
            <div>
              <label for="promptText">Prompt (what should Hexona extract / focus on?)</label>
              <textarea id="promptText" maxlength="800" placeholder="e.g. Confirm senior stakeholder exposure, MI/reporting strength, and whether commute/hybrid pattern is realistic."></textarea>
              <div class="hint"><span id="charCount">0</span>/800</div>
            </div>
          </div>

          <div class="inline" style="justify-content:space-between; align-items:center;">
            <button class="btn primary" id="runBtn">Run AI match</button>
            <div class="hint" id="statusMsg" aria-live="polite"></div>
          </div>

          <div id="processing" class="processing" aria-live="polite">
            <div class="spinner" aria-hidden="true"></div>
            <div>
              <div style="font-weight:800;color:var(--text-primary)">Processing…</div>
              <div class="hint" id="processingMsg">Analysing candidates and generating recruiter-grade notes.</div>
            </div>
          </div>

          <div id="results" class="results">
            <div class="results-summary">
              <div class="text" id="resultsSummary">Returned candidates.</div>
              <div class="jump-buttons">
                <button class="btn success" id="jumpActive">Active candidates</button>
                <button class="btn purple" id="jumpPassive">Passive candidates</button>
              </div>
            </div>

            <div class="section active" id="activeSection">
              <div class="section-head">
                <h3 id="activeTitle">Active candidates</h3>
                <span class="badge" id="activeCountBadge">0</span>
              </div>
              <div id="activeList"></div>
            </div>

            <div class="section passive" id="passiveSection">
              <div class="section-head">
                <h3 id="passiveTitle">Passive candidates</h3>
                <span class="badge" id="passiveCountBadge">0</span>
              </div>
              <div id="passiveList"></div>
            </div>

            <div class="download-row">
              <button class="btn ghost" id="downloadCsvBtn">Download CSV</button>
              <button class="btn ghost" id="downloadTxtBtn">Download TXT</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- Email Modal -->
  <div class="overlay" id="emailOverlay" aria-hidden="true">
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="emailModalTitle">
      <div class="modal-head">
        <h3 id="emailModalTitle">Email</h3>
        <button class="btn small ghost" id="emailCloseBtn" aria-label="Close">✕</button>
      </div>
      <div class="modal-body">
        <div class="modal-grid">
          <div>
            <label for="emailTo">To</label>
            <input type="text" id="emailTo" />
          </div>
          <div>
            <label for="emailSubject">Subject</label>
            <input type="text" id="emailSubject" />
          </div>
          <div>
            <label for="emailBody">Email</label>
            <textarea id="emailBody" class="email-textarea"></textarea>
          </div>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn ghost" id="copyEmailBtn">Copy</button>
        <button class="btn primary" id="sendEmailBtn">Send email</button>
      </div>
    </div>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);

    const runBtn = $('runBtn');
    const processing = $('processing');
    const results = $('results');

    const countInput = $('count');
    const countError = $('countError');
    const townInput = $('town');
    const radiusInput = $('radius');

    const jobId = $('jobId');
    const noJobId = $('noJobId');

    const scopeActive = $('scopeActive');
    const scopeInclude = $('scopeInclude');

    const promptText = $('promptText');
    const charCount = $('charCount');

    const statusMsg = $('statusMsg');
    const resultsSummary = $('resultsSummary');

    const activeSection = $('activeSection');
    const passiveSection = $('passiveSection');
    const activeList = $('activeList');
    const passiveList = $('passiveList');

    const activeCountBadge = $('activeCountBadge');
    const passiveCountBadge = $('passiveCountBadge');
    const activeTitle = $('activeTitle');
    const passiveTitle = $('passiveTitle');

    const jumpActive = $('jumpActive');
    const jumpPassive = $('jumpPassive');

    const downloadCsvBtn = $('downloadCsvBtn');
    const downloadTxtBtn = $('downloadTxtBtn');

    const emailOverlay = $('emailOverlay');
    const emailModalTitle = $('emailModalTitle');
    const emailCloseBtn = $('emailCloseBtn');
    const emailTo = $('emailTo');
    const emailSubject = $('emailSubject');
    const emailBody = $('emailBody');
    const copyEmailBtn = $('copyEmailBtn');
    const sendEmailBtn = $('sendEmailBtn');

    const toast = $('toast');

    let cachedJobId = '';
    let includeWiderPassive = false; // scope toggle
    let lastGenerated = { active: [], passive: [], all: [] };
    let activeEmailContext = null;

    // --- UI helpers ---
    function showToast(msg){
      toast.textContent = msg;
      toast.classList.add('show');
      window.clearTimeout(showToast._t);
      showToast._t = window.setTimeout(() => toast.classList.remove('show'), 2400);
    }

    function syncCharCount(){
      charCount.textContent = String((promptText.value || '').length);
    }
    promptText.addEventListener('input', syncCharCount);
    syncCharCount();

    function syncJobIdUI(){
      if (noJobId.checked) {
        cachedJobId = jobId.value.trim();
        jobId.value = '';
        jobId.readOnly = true;
        jobId.placeholder = 'No job ID selected';
      } else {
        jobId.readOnly = false;
        jobId.placeholder = 'e.g. JOB-2025-001';
        if (!jobId.value && cachedJobId) jobId.value = cachedJobId;
      }
    }
    noJobId.addEventListener('change', syncJobIdUI);
    syncJobIdUI();

    function setScope(isInclude){
      includeWiderPassive = isInclude;
      scopeInclude.classList.toggle('active', isInclude);
      scopeActive.classList.toggle('active', !isInclude);
      scopeInclude.setAttribute('aria-selected', String(isInclude));
      scopeActive.setAttribute('aria-selected', String(!isInclude));
    }
    scopeActive.addEventListener('click', () => setScope(false));
    scopeInclude.addEventListener('click', () => setScope(true));
    setScope(false);

    jumpActive.addEventListener('click', () => activeSection.scrollIntoView({behavior:'smooth', block:'start'}));
    jumpPassive.addEventListener('click', () => passiveSection.scrollIntoView({behavior:'smooth', block:'start'}));

    // --- Candidate generation ---
    const candidateNames = [
      "Georgia Patel", "Allan McBride", "Sofia Ahmed", "Daniel Cook", "Priya Desai",
      "James O'Connor", "Isla Henderson", "Theo Barrett", "Maya Shaw", "Oliver Grant",
      "Ava Kaur", "Lucas Bell", "Ethan Harris", "Grace Mitchell", "Noah Fraser"
    ];

    const sectors = ["FinTech","Healthcare","E-commerce","Manufacturing","Public Sector","Education","Energy","Media"];
    const roles = ["Account Manager","Project Lead","Implementation Consultant","Recruiter","Sales Executive","Business Analyst","Operations Coordinator","Customer Success Manager"];
    const tech = ["Salesforce","HubSpot","Excel/PowerQuery","SQL","Power BI","Looker","Outreach","Zapier/Make","Greenhouse","Workday"];
    const frameworks = ["STAR methodology","Agile/Scrum","Lean","OKRs","RACI","KPI dashboards"];
    const kpis = ["time-to-fill","pipeline coverage","NPS","win-rate","churn","SLAs","forecast accuracy"];
    const stakeholders = ["C-suite","Heads of Department","hiring managers","procurement","legal","finance"];

    function randomFrom(arr){ return arr[Math.floor(Math.random() * arr.length)]; }
    function pickMany(arr, n){
      const copy = [...arr];
      const out = [];
      for (let i = 0; i < n && copy.length; i++){
        out.push(copy.splice(Math.floor(Math.random()*copy.length), 1)[0]);
      }
      return out;
    }

    function makeEmail(name){
      const [first, last] = name.replace(" Jr.","").split(' ');
      return `${(first||'candidate').toLowerCase()}.${(last||'demo').toLowerCase()}@example.com`;
    }

    function buildPromptInsights(prompt, c){
      const p = (prompt || '').toLowerCase().trim();
      if (!p) return [];

      const out = [];
      const wantsStakeholders = /stakeholder|c-?suite|exec|influenc|senior/.test(p);
      const wantsMI = /\bmi\b|report|dashboard|kpi|metrics|data/.test(p);
      const wantsAutomation = /automation|zapier|make|workflow|process/.test(p);
      const wantsCommute = /commute|hybrid|onsite|on-site|travel|distance/.test(p);
      const wantsScale = /scale|stretch|bigger|portfolio|volume|territory/.test(p);

      if (wantsStakeholders) out.push(`Stakeholders: credible exposure working with ${c.primaryStake}; prep one “influence-up” story with measurable impact.`);
      if (wantsMI) out.push(`MI / reporting: comfortable tracking ${c.primaryKpi} and ${c.secondaryKpi}; propose a simple weekly view + actions taken off the numbers.`);
      if (wantsAutomation) out.push(`Automation: has used ${c.automationTool} to remove admin and standardise follow-up; can translate this into repeatable SOPs.`);
      if (wantsScale) out.push(`Scale: currently handles ~${c.portfolioSize} live items; position appetite for stretch with a plan for prioritisation + stakeholder comms.`);
      if (wantsCommute && c.town !== "—") out.push(`Commute: ~${c.distanceKm} km from ${c.town}; call out flexibility (hybrid days / travel windows) early.`);

      // Ensure at least 2 bullets when prompt is specific but regex misses
      if (out.length < 2) {
        out.push(`Role fit: strongest alignment is ${c.recentRole} delivery plus measurable outcomes on ${c.primaryKpi}.`);
        out.push(`Risk/mitigation: main gap is ${c.gapTopic}; offset with a 30–60–90 day plan and a small “prove-it” project.`);
      }

      return out.slice(0, 3);
    }

    function buildConsultantAdvice(c){
      // “Top class recruiter” style: framing + proof + questions + close
      const advice = [];
      advice.push(`Frame it: lead with the brief’s priority outcome (e.g. improving ${c.primaryKpi}) then link your actions and results in a tight STAR.`);
      advice.push(`Proof points: bring 2 numbers (baseline → result) and 1 artefact (dashboard snapshot / SOP / workflow) to show you’ve done it, not just spoken about it.`);
      advice.push(`De-risk the gap: be direct about ${c.gapTopic}, then present a 30–60–90 day plan (training, shadowing, quick-win project, check-ins).`);
      advice.push(`Ask smart questions: “What does great look like by week 4?” “Which stakeholders are hardest to influence?” “Which metric matters most this quarter?”`);
      advice.push(`Close properly: confirm interest, confirm constraints (notice period, commute/hybrid, comp), and agree the next step before leaving the call.`);
      return advice.slice(0, 4);
    }

    function generateCandidate(i, type, town, radiusKm, prompt){
      const nameBase = candidateNames[i % candidateNames.length] + (i >= candidateNames.length ? " Jr." : "");
      const firstName = nameBase.split(' ')[0];

      const years = Math.floor(Math.random() * 7) + 3; // 3–9
      const sector = randomFrom(sectors);
      const recentRole = randomFrom(roles);
      const prevRole = randomFrom(roles);

      const topTech = pickMany(tech, 3);
      const usedFrameworks = pickMany(frameworks, 2);

      const primaryStake = randomFrom(stakeholders);
      const primaryKpi = randomFrom(kpis);
      const secondaryKpi = randomFrom(kpis.filter(k => k !== primaryKpi));

      const distance = Math.floor(Math.random() * Math.max(5, radiusKm));
      const portfolioSize = Math.floor(Math.random() * 5) + 5; // 5–9

      const expScore = (Math.random() * 2.2 + 6.8).toFixed(1);
      const skillScore = (Math.random() * 2.2 + 6.6).toFixed(1);

      const automationTool = randomFrom(["Make", "Zapier"]);

      const gapTopic = randomFrom([...tech, ...frameworks]);
      const shortfalls = [
        `Depth in ${gapTopic} is “partial ownership” rather than full end-to-end accountability.`,
        `Less evidence of influencing ${primaryStake} on strategic decisions (more operational examples to date).`
      ];

      const experienceBullets = [
        `${years} years in ${sector}; recent focus in a ${recentRole}-type remit (previously ${prevRole}).`,
        `Manages ~${portfolioSize} live items; delivers against outcomes with ${primaryStake}.`,
        town ? `Location: ~${distance} km from ${town} (hybrid/on-site looks workable).` : `Location: radius-based match generated (no town provided).`
      ];

      const skillsBullets = [
        `Tooling: ${topTech[0]}, ${topTech[1]} (+ ${topTech[2]} where needed).`,
        `Operating rhythm: ${usedFrameworks[0]} and ${usedFrameworks[1]} to drive follow-up and accountability.`,
        `Metrics-led: comfortable reporting on ${primaryKpi} and ${secondaryKpi}; turns data into actions.`
      ];

      const promptBullets = buildPromptInsights(prompt, {
        town: town || "—",
        distanceKm: distance,
        primaryStake,
        primaryKpi,
        secondaryKpi,
        automationTool,
        portfolioSize,
        gapTopic,
        recentRole
      });

      return {
        id: `cand_${type}_${i}_${Date.now()}_${Math.random().toString(16).slice(2)}`,
        type,
        name: nameBase,
        email: makeEmail(nameBase),
        firstName,
        town: town || "—",
        distanceKm: distance,

        years, sector, recentRole, prevRole,
        topTech, usedFrameworks,
        primaryStake, primaryKpi, secondaryKpi,
        portfolioSize,
        automationTool,
        gapTopic,

        experienceScore: expScore,
        skillScore: skillScore,

        experienceBullets,
        skillsBullets,
        shortfallsBullets: shortfalls.slice(0, 2),
        adviceBullets: buildConsultantAdvice({ primaryKpi, secondaryKpi, gapTopic }),
        promptBullets,

        shortlisted: false
      };
    }

    function computeSplit(total, widerPool){
      // Always >=2 passive for demo
      const passiveMin = 2;
      const activeMin = 3; // with total >=5

      if (total < 5) return { active: 0, passive: 0 };

      let passive;
      if (widerPool) {
        // include wider pool => more passive, but keep >=3 active
        passive = Math.max(passiveMin, Math.round(total * 0.4));
        passive = Math.min(passive, total - activeMin);
      } else {
        // prefer active => minimum passive
        passive = passiveMin;
      }

      const active = total - passive;
      return { active, passive };
    }

    // --- Rendering ---
    function renderCandidate(c){
      const typePill = c.type === 'active'
        ? `<span class="pill active">Active candidate that applied</span>`
        : `<span class="pill passive">Passive candidate from pool</span>`;

      const shortlistedPill = c.shortlisted ? `<span class="pill shortlisted">Shortlisted</span>` : '';

      const actionsHtml = c.type === 'active'
        ? `
          <button class="btn primary" data-action="email-shortlisted" data-id="${c.id}">Send application shortlisted email</button>
          <button class="btn success" data-action="move-shortlist" data-id="${c.id}">Move candidate to shortlist (job pipeline)</button>
        `
        : `
          <button class="btn purple" data-action="email-interest" data-id="${c.id}">Send candidate personalised email to check if interested</button>
          <button class="btn success" data-action="move-shortlist" data-id="${c.id}">Move candidate to shortlist (job pipeline)</button>
        `;

      const expScore = `<span class="pill" title="Experience score">${c.experienceScore}/10 exp</span>`;
      const skillScore = `<span class="pill" title="Skills score">${c.skillScore}/10 skills</span>`;

      const blocks = `
        <div class="grid-mini">
          <div class="block">
            <h4 class="exp">Experience match</h4>
            <ul class="bullets">
              ${c.experienceBullets.map(b => `<li>${escapeHtml(b)}</li>`).join('')}
            </ul>
          </div>
          <div class="block">
            <h4>Skills match</h4>
            <ul class="bullets">
              ${c.skillsBullets.map(b => `<li>${escapeHtml(b)}</li>`).join('')}
            </ul>
          </div>
          <div class="block">
            <h4>Potential shortfalls</h4>
            <ul class="bullets">
              ${c.shortfallsBullets.map(b => `<li>${escapeHtml(b)}</li>`).join('')}
            </ul>
          </div>
          <div class="block">
            <h4>Recruiter interview advice</h4>
            <ul class="bullets">
              ${c.adviceBullets.map(b => `<li>${escapeHtml(b)}</li>`).join('')}
            </ul>
          </div>
          <div class="block" style="grid-column:1/-1;">
            <h4>Custom prompt insights</h4>
            <ul class="bullets">
              ${c.promptBullets.map(b => `<li>${escapeHtml(b)}</li>`).join('')}
            </ul>
          </div>
        </div>
      `;

      return `
        <div class="candidate" data-candidate="${c.id}">
          <div class="cand-top">
            <div>
              <div class="cand-name">
                ${escapeHtml(c.name)}
                ${typePill}
                ${shortlistedPill}
                ${expScore}
                ${skillScore}
              </div>
              <div class="cand-meta">Distance: ${c.distanceKm} km${c.town !== "—" ? ` from ${escapeHtml(c.town)}` : ''} · Email: ${escapeHtml(c.email)}</div>
            </div>
            <div class="cand-actions">
              ${actionsHtml}
            </div>
          </div>
          ${blocks}
        </div>
      `;
    }

    function escapeHtml(str){
      const div = document.createElement('div');
      div.textContent = str ?? '';
      return div.innerHTML;
    }

    function renderResults(active, passive, radius, town, jobIdValue){
      activeList.innerHTML = active.map(renderCandidate).join('');
      passiveList.innerHTML = passive.map(renderCandidate).join('');

      activeCountBadge.textContent = String(active.length);
      passiveCountBadge.textContent = String(passive.length);

      activeTitle.textContent = `Active candidates`;
      passiveTitle.textContent = `Passive candidates`;

      jumpActive.textContent = `Active candidates (${active.length})`;
      jumpPassive.textContent = `Passive candidates (${passive.length})`;

      const jobBit = jobIdValue ? ` for ${jobIdValue}` : '';
      const townBit = town ? ` within ${radius} km of ${town}` : ` within ${radius} km`;
      resultsSummary.textContent = `Returned ${active.length + passive.length} candidates (Active: ${active.length}, Passive: ${passive.length})${townBit}${jobBit}.`;

      results.style.display = 'block';
      results.scrollIntoView({behavior:'smooth', block:'start'});

      // Wire up buttons
      results.querySelectorAll('[data-action]').forEach(btn => {
        btn.addEventListener('click', () => {
          const action = btn.getAttribute('data-action');
          const id = btn.getAttribute('data-id');
          const cand = lastGenerated.all.find(x => x.id === id);
          if (!cand) return;

          if (action === 'move-shortlist') {
            if (cand.shortlisted) {
              showToast(`${cand.name} is already shortlisted.`);
              return;
            }
            cand.shortlisted = true;
            rerenderAfterStateChange();
            showToast(`Moved ${cand.name} to shortlist (job pipeline).`);
            return;
          }

          if (action === 'email-interest') {
            openEmailModal('passive_interest', cand);
            return;
          }
          if (action === 'email-shortlisted') {
            openEmailModal('active_shortlisted', cand);
            return;
          }
        });
      });
    }

    function rerenderAfterStateChange(){
      const town = townInput.value.trim();
      const radius = parseInt(radiusInput.value, 10) || 50;
      const jobIdValue = noJobId.checked ? '' : jobId.value.trim();
      renderResults(lastGenerated.active, lastGenerated.passive, radius, town, jobIdValue);
    }

    // --- Email templates + modal ---
    function buildEmail(mode, cand, ctx){
      const jobIdValue = ctx.jobIdValue ? ctx.jobIdValue : 'this role';
      const townLine = ctx.town ? `The role is based around ${ctx.town} (hybrid options depending on the client).` : '';
      const promptLine = ctx.prompt ? `Key points we’re aligning on: ${ctx.prompt}` : '';

      if (mode === 'passive_interest') {
        return {
          title: 'Passive outreach email',
          to: cand.email,
          subject: `Quick check – are you open to a new opportunity? (${jobIdValue})`,
          body:
`Hi ${cand.firstName},

Hope you're well.

I’m reaching out because your background looks relevant for ${jobIdValue}. ${townLine}

From your experience, it looks like you’ve worked with ${cand.primaryStake} and can move outcomes like ${cand.primaryKpi}. ${promptLine}

Would you be open to a quick 10–15 minute call this week to sense-check fit and see if it’s worth exploring?
If yes, what does your availability look like over the next 2–3 working days?

Best,
Ash`
        };
      }

      // active shortlisted
      return {
        title: 'Application shortlisted email',
        to: cand.email,
        subject: 'Update on your application',
        body:
`Hi ${cand.firstName},

Thanks again for applying.

I’m pleased to let you know your application has been shortlisted for the next stage.

Next step: I’d like to book a short call to confirm availability, role-fit, and a few key points from the brief.
If you could reply with your availability over the next 2–3 working days (or a preferred time window), I’ll get this scheduled.

Best,
Ash`
      };
    }

    function openEmailModal(mode, cand){
      const ctx = {
        town: townInput.value.trim(),
        jobIdValue: (noJobId.checked ? '' : jobId.value.trim()),
        prompt: promptText.value.trim()
      };

      const email = buildEmail(mode, cand, ctx);

      activeEmailContext = { mode, candId: cand.id };

      emailModalTitle.textContent = email.title;
      emailTo.value = email.to;
      emailSubject.value = email.subject;
      emailBody.value = email.body;

      emailOverlay.classList.add('show');
      emailOverlay.setAttribute('aria-hidden', 'false');

      // Ensure textarea shows from top and is fully visible
      emailBody.scrollTop = 0;
      requestAnimationFrame(() => emailBody.scrollTop = 0);
    }

    function closeEmailModal(){
      emailOverlay.classList.remove('show');
      emailOverlay.setAttribute('aria-hidden', 'true');
      activeEmailContext = null;
    }

    emailCloseBtn.addEventListener('click', closeEmailModal);
    emailOverlay.addEventListener('click', (e) => {
      if (e.target === emailOverlay) closeEmailModal();
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && emailOverlay.classList.contains('show')) closeEmailModal();
    });

    copyEmailBtn.addEventListener('click', async () => {
      const text = `Subject: ${emailSubject.value}\n\n${emailBody.value}`;
      try{
        await navigator.clipboard.writeText(text);
        showToast('Copied email to clipboard.');
      }catch{
        showToast('Copy failed (browser permissions).');
      }
    });

    sendEmailBtn.addEventListener('click', () => {
      if (!activeEmailContext) return;
      const cand = lastGenerated.all.find(x => x.id === activeEmailContext.candId);
      showToast(`Email sent to ${cand ? cand.name : 'candidate'}.`);
      closeEmailModal();
    });

    // --- Downloads ---
    function toCSV(all){
      const headers = [
        "type","name","email","distance_km","town",
        "experience_score","skill_score",
        "experience_bullets","skills_bullets","shortfalls","advice","prompt_insights",
        "shortlisted"
      ];
      const rows = all.map(c => ([
        c.type,
        c.name,
        c.email,
        c.distanceKm,
        c.town,
        c.experienceScore,
        c.skillScore,
        c.experienceBullets.join(" | "),
        c.skillsBullets.join(" | "),
        c.shortfallsBullets.join(" | "),
        c.adviceBullets.join(" | "),
        c.promptBullets.join(" | "),
        c.shortlisted ? "yes" : "no"
      ]).map(v => `"${String(v).replace(/"/g,'""')}"`).join(","));
      return headers.join(",") + "\n" + rows.join("\n");
    }

    function toTXT(all){
      return all.map(c => [
        `${c.name} (${c.type.toUpperCase()})`,
        `Email: ${c.email}`,
        `Distance: ${c.distanceKm} km${c.town !== "—" ? ` from ${c.town}` : ""}`,
        `Experience (${c.experienceScore}/10):`,
        ...c.experienceBullets.map(b => `- ${b}`),
        `Skills (${c.skillScore}/10):`,
        ...c.skillsBullets.map(b => `- ${b}`),
        `Potential shortfalls:`,
        ...c.shortfallsBullets.map(b => `- ${b}`),
        `Recruiter interview advice:`,
        ...c.adviceBullets.map(b => `- ${b}`),
        `Custom prompt insights:`,
        ...c.promptBullets.map(b => `- ${b}`),
        `Shortlisted: ${c.shortlisted ? "Yes" : "No"}`,
        `---`
      ].join("\n")).join("\n\n");
    }

    function downloadFile(content, filename, mime){
      const blob = new Blob([content], { type: mime });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    downloadCsvBtn.addEventListener('click', () => {
      if (!lastGenerated.all.length) return;
      downloadFile(toCSV(lastGenerated.all), 'hexona_candidate_match_demo.csv', 'text/csv');
    });

    downloadTxtBtn.addEventListener('click', () => {
      if (!lastGenerated.all.length) return;
      downloadFile(toTXT(lastGenerated.all), 'hexona_candidate_match_demo.txt', 'text/plain');
    });

    // --- Run match ---
    runBtn.addEventListener('click', () => {
      countError.style.display = 'none';

      const town = townInput.value.trim();
      const radius = parseInt(radiusInput.value, 10) || 50;
      const total = parseInt(countInput.value, 10);

      if (!total || total < 5){
        countError.style.display = 'block';
        countInput.focus();
        return;
      }

      const jobIdValue = noJobId.checked ? '' : jobId.value.trim();

      results.style.display = 'none';
      processing.style.display = 'flex';
      statusMsg.textContent = '';
      runBtn.disabled = true;

      const split = computeSplit(total, includeWiderPassive);
      const prompt = promptText.value.trim();

      window.setTimeout(() => {
        const active = [];
        const passive = [];

        for (let i = 0; i < split.active; i++){
          active.push(generateCandidate(i, 'active', town, radius, prompt));
        }
        for (let j = 0; j < split.passive; j++){
          passive.push(generateCandidate(j + split.active, 'passive', town, radius, prompt));
        }

        lastGenerated = { active, passive, all: [...active, ...passive] };

        processing.style.display = 'none';
        runBtn.disabled = false;

        renderResults(active, passive, radius, town, jobIdValue);
      }, 1200);
    });

    // Initialize
    statusMsg.textContent = 'Ready when you are.';
  </script>
</body>
</html>
