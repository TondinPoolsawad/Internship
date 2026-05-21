// scrapers/dede.js — runs scraper.js via spawn (ESM), then cleandata.py
const { spawn, execFile } = require('child_process');
const path = require('path');

process.on('message', async ({ config }) => {
  const { startYear, endYear, output } = config;
  const scraperPath = path.join(__dirname, '..', 'scripts', 'scraper.mjs');
  const cleanerPath = path.join(__dirname, '..', 'scripts', 'cleandata.py');

  process.stdout.write(`Starting DEDE scraper (${startYear}–${endYear})...\n`);

  // Step 1: Run scraper.js via node (ESM) to download XLSX/PDF files
  process.stdout.write(`\n[1/2] Downloading energy balance files from dede.go.th...\n`);

  await new Promise((resolve, reject) => {
    // Use spawn with node so ESM import works
    const child = spawn('node', [scraperPath], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: {
        ...process.env,
        DEDE_MIN_YEAR: String(startYear),
        DEDE_MAX_YEAR: String(endYear),
      },
    });
    child.stdout.on('data', d => process.stdout.write(d.toString()));
    child.stderr.on('data', d => process.stdout.write('⚠ ' + d.toString()));
    child.on('exit', code => code === 0 ? resolve() : reject(new Error(`scraper exited ${code}`)));
    child.on('error', reject);
  });

  // Step 2: Run cleandata.py
  process.stdout.write(`\n[2/2] Parsing XLSX files...\n`);

  await new Promise((resolve, reject) => {
    const child = spawn('python', [cleanerPath], {
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUTF8: '1',
        DEDE_OUTPUT: output,
      },
    });
    child.stdout.on('data', d => process.stdout.write(d.toString('utf8')));
    child.stderr.on('data', d => process.stdout.write('⚠ ' + d.toString('utf8')));
    child.on('exit', code => code === 0 ? resolve() : reject(new Error(`cleandata exited ${code}`)));
    child.on('error', reject);
  });

  process.stdout.write(`\n✅ DEDE complete → ${output}\n`);
  process.exit(0);
});
