const logEl = document.getElementById('log');
const outPathEl = document.getElementById('outPath');

function log(msg){
  logEl.textContent += msg + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

window.electronAPI.onLog((_, message) => {
  log(message);
});

document.getElementById('clear').addEventListener('click', () => {
  logEl.textContent = '';
});

document.getElementById('chooseOut').addEventListener('click', async () => {
  const res = await window.electronAPI.chooseOutput();
  if (res && res.filePath) {
    outPathEl.textContent = res.filePath;
    outPathEl.dataset.path = res.filePath;
  }
});

document.getElementById('run').addEventListener('click', async () => {
  const hs = document.getElementById('hs').value.trim();
  const start = document.getElementById('start').value;
  const end = document.getElementById('end').value;
  const types = [];
  if (document.getElementById('type-import').checked) types.push('import');
  if (document.getElementById('type-export').checked) types.push('export');
  const out = outPathEl.dataset.path;

  if (!hs || !start || !end || !types.length || !out) {
    log('⚠️ Please fill all fields and choose output file.');
    return;
  }

  const hsCodes = hs.split(',').map(s => s.trim()).filter(Boolean);
  const config = {
    yearMonth: [start, end],
    type: types,
    hsCodes,
    output: out
  };

  try {
    log('▶️ Starting...');
    const ok = await window.electronAPI.runScraper(config);
    if (ok) log('✅ Done.');
    else log('⚠️ Finished with warnings.');
  } catch (e) {
    log('❌ Error: ' + e.message);
  }
});
