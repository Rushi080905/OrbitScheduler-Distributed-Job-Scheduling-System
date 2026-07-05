🚀 OrbitScheduler: Distributed Job Scheduling System

OrbitScheduler is a distributed, fault-tolerant job orchestration engine and real-time monitoring dashboard. Built with React (Vite), Node.js (Express), and WebSockets, it demonstrates core enterprise system design principles like concurrent worker nodes, atomic database locks, and exponential backoff retry policies.

🛠️ Setup & Execution

The project is built using a monorepo structure with Node.js (Express) and a React (Vite) frontend.

Install Dependencies:

npm install


Environment Configuration:
Copy the provided .env.example to .env and insert the required GEMINI_API_KEY.

Run the Full-Stack Application:

# This runs both the Vite frontend and Express backend concurrently via tsx
npm run dev


Access the Dashboard: Open http://localhost:3000 in your browser.

Project Structure Overview:

server.ts: The main Express backend and WebSocket server entry point.

src/routes/: Contains REST API endpoint definitions (e.g., job processing routes).

src/workers/: Contains the simulated distributed worker node logic and polling mechanics.

src/db/: Contains the simulated PostgreSQL database state and locking logic.

src/main.tsx & src/App.tsx: The React (Vite) frontend entry points and dashboard UI components.

🏛️ High-Level Architecture Diagram

The system decouples the Client layer, API Gateway, Metadata Database, and a scalable Worker Swarm. State changes are pushed to the client via WebSockets.





+---------------------------------------------------+
|                 CLIENT TIER                       |
|  +---------------------------------------------+  |
|  |          React Web Dashboard                |  |
|  +---------------------------------------------+  |
+-------------------------+-------------------------+
                          | 
                          | REST API (HTTP POST)
                          | WebSockets (socket.io)
                          v
+---------------------------------------------------+
|               APPLICATION TIER                    |
|  +--------------------+   +--------------------+  |
|  | Express API Server |   |  WebSocket Server  |  |
|  +--------------------+   +--------------------+  |
+-------------------------+-------------------------+
                          | 
                          | Read / Write State
                          | Event Triggers
                          v
+---------------------------------------------------+
|               PERSISTENCE TIER                    |
|  +---------------------------------------------+  |
|  |    Metadata Database (Simulated Postgres)   |  |
|  +---------------------------------------------+  |
+-------------------------+-------------------------+
                          ^ 
                          | Polls / Atomic Claims
                          | (SELECT SKIP LOCKED)
                          v
+---------------------------------------------------+
|                  COMPUTE TIER                     |
|  +------------+  +------------+  +-------------+  |
|  | Worker 1   |  | Worker 2   |  | Worker N    |  |
|  +------------+  +------------+  +-------------+  |
+---------------------------------------------------+






🗄️ Entity-Relationship (ER) Diagram

The schema relies on row-level locks and strict state machines to prevent race conditions during concurrent worker polling.




      +-------------------------+
      |         QUEUE           |
      +-------------------------+
      | PK  queue_id            |
      |     name                |
      |     concurrency_limit   |
      |     retry_strategy      |
      +-----------+-------------+
                  |
                  | 1
                  |
                  | contains
                  |
                  | N
+-----------------v-----------------+          +-------------------------+
|                JOB                |          |      WORKER_NODE        |
+-----------------------------------+          +-------------------------+
| PK  id                            |          | PK  id                  |
| FK  queue_id                      |          |     status              |
|     type                          |  N    1  |     processedCount      |
|     payload                       |----------|     failedCount         |
|     status (queued/running/etc)   | executes |     uptime              |
|     attempts                      |          +-------------------------+
|     max_retries                   |
|     run_at                        |
| FK  locked_by (Worker ID)         |
|     ai_summary                    |
+-----------------------------------+






🔌 API Documentation

The Express backend (server.ts) exposes the following REST endpoints for cluster management:

Job & Queue Management

POST /api/seed - Seeds the database with mock jobs across different queues (Video Encoding, Data Pipelines, Email Delivery).

POST /api/clear - Truncates the jobs, executions, and dead letter queues.

GET /api/state - Dumps the entire in-memory database state for debugging and inspection.

Worker Node Management

GET /api/workers - Returns the real-time status, processed count, failed count, and uptime of all active worker nodes in the swarm.

POST /api/workers - Spawns a new simulated worker node and attaches it to the cluster.

POST /api/workers/:id/toggle - Pauses or resumes a specific worker node to simulate node degradation.

POST /api/workers/:id/delete - Permanently terminates a worker node instance.

Real-Time Telemetry (WebSockets)

Event: job_updated

Trigger: Emitted via Socket.io whenever a job changes state or workers process a queue.

🧠 Design Decisions & Major Trade-offs

Concurrency: "SKIP LOCKED" vs Distributed Locking (Redis)

Decision: Used a simulated SELECT FOR UPDATE SKIP LOCKED database pattern for task claiming.

Trade-off: We avoided external distributed lock managers (like Redis Redlock) to reduce infrastructure complexity. Using a relational database's row-level locking ensures that task metadata and lock state are perfectly transactional without a two-phase commit, though it sacrifices a minor amount of throughput compared to pure in-memory Redis locks.

Real-time Telemetry: WebSockets vs HTTP Polling

Decision: Implemented Socket.io to push state changes to the React dashboard.

Trade-off: HTTP polling is easier to implement across stateless servers, but it creates massive, unnecessary read-load on the database. WebSockets require stateful sticky sessions but drastically reduce database reads and provide a buttery-smooth UX for monitoring active workers.

Fault Tolerance: Exponential Backoff vs Fixed Retries

Decision: Failed jobs utilize an Exponential Backoff strategy before hitting the Dead Letter Queue (DLQ).

Trade-off: Fixed retries process failures faster. However, in a real-world scenario (like a downstream API going offline), fixed retries cause a "thundering herd" effect that can crash the recovering API. Exponential backoff trades immediate retry speed for systemic stability.

🧪 Automated Tests

A lightweight Node.js test suite using the native assert module validates the core engine logic. Run the suite via:

npx tsx tests/scheduler.test.ts


Test Coverage:

Job Enqueueing: Verifies jobs are correctly inserted with a queued status and initialized attempt counters.

Concurrency Locks: Verifies that when a worker claims a job, the status safely transitions to running and the locked_by attribute is atomically assigned to the specific Worker ID.

Queue Exhaustion: Ensures workers gracefully idle and return null when no jobs are available, preventing infinite polling loops.
