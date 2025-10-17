const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { runScraperWithLogger } = require('./scraper');

let mainWindow;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 900,
    height: 700,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  mainWindow.loadFile('index.html');
}

app.whenReady().then(() => {
  createWindow();

  app.on('activate', function () {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', function () {
  if (process.platform !== 'darwin') app.quit();
});

ipcMain.handle('choose-output', async () => {
  const { filePath, canceled } = await dialog.showSaveDialog(mainWindow, {
    title: 'Save CSV As',
    defaultPath: 'customs_data.csv',
    filters: [{ name: 'CSV', extensions: ['csv'] }]
  });
  if (canceled) return null;
  return { filePath };
});

ipcMain.handle('run-scraper', async (_event, config) => {
  const log = (m) => mainWindow && mainWindow.webContents.send('log', m);
  try {
    await runScraperWithLogger(config, log);
    return true;
  } catch (e) {
    log('❌ ' + e.message);
    return false;
  }
});
