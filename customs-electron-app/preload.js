const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  chooseOutput: () => ipcRenderer.invoke('choose-output'),
  runScraper: (config) => ipcRenderer.invoke('run-scraper', config),
  onLog: (cb) => ipcRenderer.on('log', cb)
});
