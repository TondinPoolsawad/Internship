// renderer.js

// ── Tab switching ──
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.querySelector(`[data-panel="${tab.dataset.tab}"]`).classList.add('active');
  });
});

// ── Status bar clock ──
function updateClock() {
  const now = new Date();
  document.getElementById('sb-time').textContent =
    now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}
setInterval(updateClock, 1000);
updateClock();

// ── Log helpers ──
function appendLog(el, msg) {
  const line = document.createElement('span');
  if (msg.includes('✅') || msg.includes('☁️') || msg.includes('Done')) {
    line.className = 'ok';
  } else if (msg.includes('⚠') || msg.includes('N/A') || msg.includes('not found')) {
    line.className = 'warn';
  } else if (msg.includes('Error') || msg.includes('ERROR') || msg.includes('❌')) {
    line.className = 'err';
  } else if (msg.startsWith('Config') || msg.startsWith('Ready')) {
    line.className = 'info';
  }
  line.textContent = msg;
  el.appendChild(line);
  el.appendChild(document.createTextNode('\n'));
  el.scrollTop = el.scrollHeight;
}

function setRunning(prefix, running) {
  const dot = document.getElementById(`${prefix}-dot`);
  const status = document.getElementById(`${prefix}-status`);
  const runBtn = document.getElementById(`${prefix}-run`);
  dot.className = 'status-dot' + (running ? ' running' : '');
  status.textContent = running ? 'Running…' : 'Done';
  if (runBtn) runBtn.disabled = running;
}

// ── Generic scraper runner ──
async function runScraper(prefix, scraper, buildConfig) {
  const logEl = document.getElementById(`${prefix}-log`);
  const outPath = document.getElementById(`${prefix}-outpath`).dataset.path;

  if (!outPath) {
    appendLog(logEl, '⚠ Please choose an output file first.');
    return;
  }

  const config = buildConfig(outPath);
  if (!config) return;

  setRunning(prefix, true);
  appendLog(logEl, `--- Starting ${new Date().toLocaleString()} ---`);

  // Clear old listeners
  window.gdias.offLog();
  window.gdias.onLog((msg) => {
    msg.split('\n').filter(Boolean).forEach(line => appendLog(logEl, line));
  });

  try {
    await window.gdias.runScraper(scraper, config);
    appendLog(logEl, `✅ Completed successfully → ${outPath}`);
  } catch (e) {
    appendLog(logEl, `❌ Error: ${e.message}`);
  } finally {
    setRunning(prefix, false);
  }
}

// ── Choose output helper ──
async function chooseOutput(prefix, defaultName) {
  const path = await window.gdias.chooseOutput(defaultName);
  if (path) {
    const el = document.getElementById(`${prefix}-outpath`);
    el.textContent = path.split(/[\\/]/).pop();
    el.dataset.path = path;
  }
}

// ═══════════════════════════════════════
// CUSTOMS
// ═══════════════════════════════════════
document.getElementById('c-choose').addEventListener('click', () => chooseOutput('c', 'customs.csv'));
document.getElementById('c-clear').addEventListener('click', () => { document.getElementById('c-log').innerHTML = ''; });
document.getElementById('c-run').addEventListener('click', () => {
  runScraper('c', 'customs.js', (output) => {
    const hs = document.getElementById('c-hs').value.trim();
    const start = document.getElementById('c-start').value;
    const end = document.getElementById('c-end').value;
    const types = [];
    if (document.getElementById('c-import').checked) types.push('import');
    if (document.getElementById('c-export').checked) types.push('export');

    if (!hs || !start || !end || !types.length) {
      appendLog(document.getElementById('c-log'), '⚠ Please fill all fields and select at least one type.');
      return null;
    }

    const hsCodes = hs.split(',').map(s => s.trim()).filter(Boolean);
    return { yearMonth: [start, end], type: types, hsCodes, output };
  });
});

// ═══════════════════════════════════════
// OAE
// ═══════════════════════════════════════
document.getElementById('o-choose').addEventListener('click', () => chooseOutput('o', 'oae.csv'));
document.getElementById('o-clear').addEventListener('click', () => { document.getElementById('o-log').innerHTML = ''; });
document.getElementById('o-run').addEventListener('click', () => {
  runScraper('o', 'oae.js', (output) => ({ output }));
});

// ═══════════════════════════════════════
// FISHERIES
// ═══════════════════════════════════════
document.getElementById('f-choose').addEventListener('click', () => chooseOutput('f', 'fisheries.csv'));
document.getElementById('f-clear').addEventListener('click', () => { document.getElementById('f-log').innerHTML = ''; });
document.getElementById('f-run').addEventListener('click', () => {
  runScraper('f', 'fisheries.js', (output) => ({ output }));
});

// ═══════════════════════════════════════
// DEDE
// ═══════════════════════════════════════
document.getElementById('d-choose').addEventListener('click', () => chooseOutput('d', 'dede.csv'));
document.getElementById('d-clear').addEventListener('click', () => { document.getElementById('d-log').innerHTML = ''; });
document.getElementById('d-run').addEventListener('click', () => {
  runScraper('d', 'dede.js', (output) => {
    const startYear = parseInt(document.getElementById('d-start').value);
    const endYear = parseInt(document.getElementById('d-end').value);
    if (!startYear || !endYear || startYear > endYear) {
      appendLog(document.getElementById('d-log'), '⚠ Invalid year range.');
      return null;
    }
    return { startYear, endYear, output };
  });
});
