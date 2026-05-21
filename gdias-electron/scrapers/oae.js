// scrapers/oae.js — calls oae_api.py via Python with output path override
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

process.on('message', ({ config }) => {
  const { output } = config;
  const scriptPath = path.join(__dirname, '..', 'scripts', 'oae_api.py');

  process.stdout.write(`Starting OAE scraper...\nOutput → ${output}\n`);

  const child = spawn('python', [scriptPath], {
    env: {
      ...process.env,
      PYTHONIOENCODING: 'utf-8',
      PYTHONUTF8: '1',
      OAE_OUTPUT: output,
    },
  });

  child.stdout.on('data', d => process.stdout.write(d.toString('utf8')));
  child.stderr.on('data', d => process.stdout.write('⚠ ' + d.toString('utf8')));

  child.on('exit', (code) => {
    if (code === 0) {
      process.stdout.write('✅ OAE done.\n');
      process.exit(0);
    } else {
      process.stdout.write(`❌ Exited with code ${code}\n`);
      process.exit(1);
    }
  });
});
