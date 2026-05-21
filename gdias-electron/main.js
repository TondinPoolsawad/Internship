const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { fork } = require('child_process');

function createWindow() {
  const win = new BrowserWindow({
    width: 1100,
    height: 750,
    minWidth: 800,
    minHeight: 600,
    backgroundColor: '#0d0f11',
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
    icon: path.join(__dirname, 'assets', 'icon.png'),
  });

  win.loadFile('index.html');
}

app.whenReady().then(createWindow);
app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });

// ── Choose output file ──
ipcMain.handle('choose-output', async (_, defaultName) => {
  const { filePath } = await dialog.showSaveDialog({
    defaultPath: defaultName || 'output.csv',
    filters: [{ name: 'CSV', extensions: ['csv'] }],
  });
  return filePath || null;
});

// ── Run a scraper script ──
// Each scraper is a standalone Node/Python script that accepts JSON config via stdin
// and streams log lines via stdout.
ipcMain.handle('run-scraper', async (event, { scraper, config }) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(__dirname, 'scrapers', scraper);
    const isNode = scraper.endsWith('.js');
    const cmd = isNode ? process.execPath : 'python';
    const args = isNode ? [scriptPath] : [scriptPath];

    const child = fork(scriptPath, [], {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      env: { ...process.env },
    });

    // Send config via IPC message
    child.send({ config });

    child.stdout.on('data', (data) => {
      event.sender.send('scraper-log', data.toString());
    });

    child.stderr.on('data', (data) => {
      event.sender.send('scraper-log', '⚠ ' + data.toString());
    });

    child.on('exit', (code) => {
      if (code === 0) resolve(true);
      else reject(new Error(`Exited with code ${code}`));
    });

    child.on('error', reject);
  });
});
