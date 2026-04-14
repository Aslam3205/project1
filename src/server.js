'use strict';

const dotenv = require('dotenv');
dotenv.config();

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN || '*';

// ─── Express App ──────────────────────────────────────────────────────────────

const app = express();

app.use(cors({ origin: CLIENT_ORIGIN }));
app.use(express.json());

// Health check
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Ace Masters Backend',
    version: '2.0.0',
    timestamp: new Date().toISOString(),
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// Global error handler
app.use((err, req, res, _next) => {
  console.error('[error]', err.stack || err.message);
  res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
});

// ─── HTTP + Socket.IO Server ──────────────────────────────────────────────────

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: CLIENT_ORIGIN,
    methods: ['GET', 'POST'],
  },
});

io.on('connection', (socket) => {
  const clientId = uuidv4();
  console.log(`[socket] client connected  id=${clientId} socket=${socket.id}`);

  socket.on('disconnect', (reason) => {
    console.log(`[socket] client disconnected id=${clientId} reason=${reason}`);
  });

  socket.on('error', (err) => {
    console.error(`[socket] error id=${clientId}`, err.message);
  });
});

// ─── Start ────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`[server] Ace Masters Backend v2.0.0 listening on port ${PORT}`);
});

server.on('error', (err) => {
  console.error('[server] fatal error:', err.message);
  process.exit(1);
});

// Graceful shutdown
const shutdown = (signal) => {
  console.log(`[server] received ${signal}, shutting down gracefully`);
  server.close(() => {
    console.log('[server] closed');
    process.exit(0);
  });
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
