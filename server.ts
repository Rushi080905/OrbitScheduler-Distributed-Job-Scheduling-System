import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import dotenv from 'dotenv';
import { createServer } from 'http';
import { Server } from 'socket.io';
import jobRoutes from './src/routes/jobRoutes.js';
import { dbState, logSystemEvent } from './src/db/index.js';
import { DistributedWorker } from './src/workers/index.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: '*',
  }
});

// Expose io globally so that db state updates can trigger events
(global as any).io = io;

io.on('connection', (socket) => {
  console.log(`[Socket.IO] Client connected: ${socket.id}`);
  socket.on('disconnect', () => {
    console.log(`[Socket.IO] Client disconnected: ${socket.id}`);
  });
});

// In-Memory Worker Management Pool
interface ActiveWorkerRecord {
  id: string;
  name: string;
  status: 'working' | 'idle' | 'paused';
  processedCount: number;
  failedCount: number;
  lastActive: Date;
  workerInstance: DistributedWorker;
  workload: string;
  uptime: string;
}

const activeWorkers: Record<string, ActiveWorkerRecord> = {};

// Helper to spawn a new simulated worker
function spawnWorker(id: string, name: string, initialStatus: 'working' | 'idle' | 'paused' = 'idle', workload = '0%', uptime = '0s') {
  if (activeWorkers[id]) return;

  const workerInstance = new DistributedWorker({
    workerId: name,
    pollIntervalMs: 5000,
  });

  activeWorkers[id] = {
    id,
    name,
    status: initialStatus,
    processedCount: initialStatus === 'working' ? 12 : 0,
    failedCount: 0,
    lastActive: new Date(),
    workerInstance,
    workload,
    uptime,
  };

  // Only start the worker instance if it is not paused
  if (initialStatus !== 'paused') {
    workerInstance.start();
  }
  logSystemEvent('info', `Simulated Distributed Worker [${name}] spawned and connected to cluster.`);
}

// Spawn initial workers to match mock screenshots exactly
spawnWorker('WRK-01', 'WRK-01', 'working', '85%', '12d');
spawnWorker('WRK-02', 'WRK-02', 'idle', '0%', '4d');
spawnWorker('WRK-03', 'WRK-03', 'working', '42%', '1d');
spawnWorker('WRK-04', 'WRK-04', 'working', '91%', '22h');
spawnWorker('WRK-05', 'WRK-05', 'idle', '0%', '15d');
spawnWorker('WRK-06', 'WRK-06', 'working', '73%', '3d');
spawnWorker('WRK-07', 'WRK-07', 'idle', '0%', '6d');
spawnWorker('WRK-08', 'WRK-08', 'working', '58%', '12h');

// Intercept original poll query inside worker if needed to record worker statistics in memory
const originalClaim = DistributedWorker.prototype['claimAndExecuteJob'];
DistributedWorker.prototype['claimAndExecuteJob'] = async function() {
  const name = this['workerId'];
  const workerRecord = Object.values(activeWorkers).find(w => w.name === name);
  
  if (workerRecord && workerRecord.status !== 'paused') {
    workerRecord.lastActive = new Date();
    workerRecord.status = 'working';
  }
  
  try {
    await originalClaim.call(this);
    if (workerRecord && workerRecord.status !== 'paused') {
      workerRecord.status = 'idle';
    }
  } catch (err) {
    if (workerRecord && workerRecord.status !== 'paused') {
      workerRecord.status = 'idle';
    }
    throw err;
  }
};

// Also listen to system logs to increment processed count
// Simple hook into logSystemEvent to count successes/failures per worker
const originalLogSystemEvent = logSystemEvent;
(global as any).logSystemEvent = function(level: 'info' | 'warn' | 'error', message: string) {
  originalLogSystemEvent(level, message);
  
  // Parse message to see if worker completed/failed a job
  // e.g., "Worker-Node-Alpha successfully completed job-901c8a12"
  if (message.includes('completed successfully')) {
    const match = message.match(/\[Worker-(.*?)\]/);
    if (match && match[1]) {
      const workerRecord = Object.values(activeWorkers).find(w => w.name === match[1]);
      if (workerRecord) {
        workerRecord.processedCount += 1;
      }
    }
  } else if (message.includes('failed. Retrying') || message.includes('exhausted retries')) {
    const match = message.match(/\[Worker-(.*?)\]/);
    if (match && match[1]) {
      const workerRecord = Object.values(activeWorkers).find(w => w.name === match[1]);
      if (workerRecord) {
        workerRecord.failedCount += 1;
      }
    }
  }
};

// API: Register Scheduler Routes
app.use('/', jobRoutes);

// API: Worker Manager Endpoints
app.get('/api/workers', (req, res) => {
  const list = Object.values(activeWorkers).map(w => ({
    id: w.id,
    name: w.name,
    status: w.status,
    processedCount: w.processedCount,
    failedCount: w.failedCount,
    lastActive: w.lastActive,
    workload: w.workload,
    uptime: w.uptime,
  }));
  res.json(list);
});

app.post('/api/workers', (req, res) => {
  const { name } = req.body;
  if (!name || typeof name !== 'string') {
    res.status(400).json({ error: 'Worker name must be a non-empty string' });
    return;
  }
  const id = 'WRK-' + Math.floor(Math.random() * 90 + 10);
  spawnWorker(id, name, 'idle', '0%', '1s');
  res.status(201).json({ message: 'Worker spawned successfully', id, name });
});

app.post('/api/workers/:id/toggle', (req, res) => {
  const { id } = req.params;
  const worker = activeWorkers[id];
  if (!worker) {
    res.status(404).json({ error: 'Worker not found' });
    return;
  }

  if (worker.status === 'paused') {
    worker.status = 'idle';
    worker.workerInstance.start();
    logSystemEvent('info', `Simulated worker [${worker.name}] resumed operation.`);
  } else {
    worker.status = 'paused';
    worker.workerInstance.stop();
    logSystemEvent('warn', `Simulated worker [${worker.name}] paused.`);
  }

  res.json({ id, name: worker.name, status: worker.status });
});

app.post('/api/workers/:id/delete', (req, res) => {
  const { id } = req.params;
  const worker = activeWorkers[id];
  if (!worker) {
    res.status(404).json({ error: 'Worker not found' });
    return;
  }

  worker.workerInstance.stop();
  delete activeWorkers[id];
  logSystemEvent('warn', `Simulated worker [${worker.name}] removed from cluster.`);
  res.json({ message: 'Worker removed successfully' });
});

// API: Fetch Database State (for playground inspection)
app.get('/api/state', (req, res) => {
  res.json({
    users: dbState.users,
    projects: dbState.projects,
    queues: dbState.queues,
    jobs: dbState.jobs,
    jobExecutions: dbState.jobExecutions,
    deadLetterQueue: dbState.deadLetterQueue,
    systemLogs: dbState.systemLogs,
  });
});

// API: Seed mock jobs
app.post('/api/seed', (req, res) => {
  const initialJobs = [
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-vid-enc',
      payload: { task: 'Transcode High-Res Stream', video_id: 'v_high_res', processing_time: 2000, format: 'mkv' },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-eml-dlv',
      payload: { task: 'Broadcast Batch Interview Invitation', recipient: 'all-candidates@edu.com', subject: 'Campus Placements Update', body: 'Shortlisted for Round 2!' },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-dta-pip',
      payload: { task: 'Fetch External API Analytics', query: 'SELECT COUNT(*) FROM placements', should_fail: true, error_reason: 'HTTP 503 Service Unavailable' },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 2,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-vid-enc',
      payload: { task: 'Apply Watermark Overlay to Lectures', processing_time: 800 },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-eml-dlv',
      payload: { task: 'Delivery Confirmation SMS Dispatcher', processing_time: 500 },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-dta-pip',
      payload: { task: 'Clean database partitions', processing_time: 4000 },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-vid-enc',
      payload: { task: 'Compile Clip Highlight reel', processing_time: 2200 },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    },
    {
      id: 'job-' + Math.random().toString(36).substring(2, 9),
      queue_id: 'q-eml-dlv',
      payload: { task: 'Feedback Survey Auto-Mailer', processing_time: 1200 },
      status: 'queued' as const,
      attempts: 0,
      max_retries: 3,
      run_at: new Date(),
      locked_by: null,
      locked_at: null,
      created_at: new Date(),
      updated_at: new Date(),
    }
  ];

  dbState.jobs.push(...initialJobs);
  logSystemEvent('info', 'Seeded 8 mock demonstration tasks onto queues.');
  io.emit('job_updated', { timestamp: new Date() });
  res.json({ message: 'Seeded 8 jobs successfully' });
});

// API: Clear queues/jobs/DLQ
app.post('/api/clear', (req, res) => {
  dbState.jobs = [];
  dbState.deadLetterQueue = [];
  dbState.jobExecutions = [];
  dbState.systemLogs = [
    {
      id: 'log-' + Math.random().toString(36).substring(2, 9),
      timestamp: new Date(),
      level: 'info',
      message: 'Database tables cleared. Ready for new operations.'
    }
  ];
  logSystemEvent('info', 'Clear command issued: Jobs and Executions tables empty.');
  io.emit('job_updated', { timestamp: new Date() });
  res.json({ message: 'All tables truncated successfully' });
});

// Setup Port
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';

// Host Vite or Static Files
async function startServer() {
  const isProduction = process.env.NODE_ENV === 'production' || !fs.existsSync(path.resolve(__dirname, 'index.html'));

  if (!isProduction) {
    console.log('Spawning Vite Dev Middleware inside Express Server...');
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: false
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    console.log('Serving pre-built production static files...');
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (req, res) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  }

  httpServer.listen(PORT, HOST, () => {
    console.log(`=======================================================`);
    console.log(`  Distributed Job Scheduler Server listening on port ${PORT}`);
    console.log(`  Local: http://localhost:${PORT}`);
    console.log(`  Platform dev server proxy target verified.            `);
    console.log(`=======================================================`);
  });
}

startServer().catch((err) => {
  console.error('Failed to start Express development server:', err);
});
