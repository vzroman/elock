import express from 'express';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {scanRuns} from './report-data.js';

const serverDirectory = path.dirname(fileURLToPath(import.meta.url));
const applicationDirectory = path.resolve(serverDirectory, '..');
const projectDirectory = path.resolve(applicationDirectory, '..');
const logsRoot = path.join(projectDirectory, '_build', 'test', 'logs');
const distDirectory = path.join(applicationDirectory, 'dist');
const port = 3000;

const app = express();

app.get('/api/report', async (_request, response, next) => {
  try {
    response.json(await scanRuns(logsRoot));
  } catch (error) {
    next(error);
  }
});

app.use('/ct-logs', express.static(logsRoot));
app.use(express.static(distDirectory));
app.use((_request, response) => {
  response.sendFile(path.join(distDirectory, 'index.html'));
});

app.listen(port, () => {
  console.log(`Performance report: http://localhost:${port}`);
});
