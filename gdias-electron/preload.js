const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('gdias', {
  chooseOutput: (defaultName) => ipcRenderer.invoke('choose-output', defaultName),
  runScraper: (scraper, config) => ipcRenderer.invoke('run-scraper', { scraper, config }),
  onLog: (cb) => ipcRenderer.on('scraper-log', (_, msg) => cb(msg)),
  offLog: () => ipcRenderer.removeAllListeners('scraper-log'),
});
