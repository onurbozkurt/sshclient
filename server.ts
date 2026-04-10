import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import * as pty from "node-pty";
import path from "path";
import { execSync } from "child_process";
import crypto from "crypto";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function findShell(): string {
    if (process.platform === "darwin") {
        try {
            return execSync("which zsh").toString().trim();
        } catch {
            return "/bin/zsh";
        }
    }
    return process.env.SHELL || "/bin/bash";
}

const shell = findShell();
console.log(`Using shell: ${shell}`);

// --- Session management ---

const MAX_SCROLLBACK = 100000; // characters to buffer per session
const SESSION_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes before orphan cleanup

interface Session {
    id: string;
    pty: pty.IPty;
    scrollback: string;
    ws: WebSocket | null;
    cleanupTimer: ReturnType<typeof setTimeout> | null;
    alive: boolean; // false once the pty process exits
}

const sessions = new Map<string, Session>();

function createSession(): Session {
    const id = crypto.randomUUID();
    const term = pty.spawn(shell, [], {
        name: "xterm-256color",
        cols: 80,
        rows: 24,
        cwd: process.env.HOME || "/",
        env: process.env as Record<string, string>,
    });

    const session: Session = {
        id,
        pty: term,
        scrollback: "",
        ws: null,
        cleanupTimer: null,
        alive: true,
    };

    term.onData((data: string) => {
        // Buffer scrollback
        session.scrollback += data;
        if (session.scrollback.length > MAX_SCROLLBACK) {
            session.scrollback = session.scrollback.slice(-MAX_SCROLLBACK);
        }
        // Forward to connected client
        if (session.ws && session.ws.readyState === WebSocket.OPEN) {
            session.ws.send(JSON.stringify({ type: "output", data }));
        }
    });

    term.onExit(() => {
        session.alive = false;
        if (session.ws && session.ws.readyState === WebSocket.OPEN) {
            session.ws.send(JSON.stringify({ type: "exit" }));
        }
        destroySession(id);
    });

    sessions.set(id, session);
    console.log(`Session created: ${id} (total: ${sessions.size})`);
    return session;
}

function destroySession(id: string) {
    const session = sessions.get(id);
    if (!session) return;
    if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
    if (session.alive) {
        session.alive = false;
        session.pty.kill();
    }
    sessions.delete(id);
    console.log(`Session destroyed: ${id} (total: ${sessions.size})`);
}

function startCleanupTimer(session: Session) {
    if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
    session.cleanupTimer = setTimeout(() => {
        console.log(`Session timed out: ${session.id}`);
        destroySession(session.id);
    }, SESSION_TIMEOUT_MS);
}

function attachWebSocket(session: Session, ws: WebSocket) {
    // Detach previous WebSocket if any
    session.ws = ws;
    if (session.cleanupTimer) {
        clearTimeout(session.cleanupTimer);
        session.cleanupTimer = null;
    }

    // Send session ID to client
    ws.send(JSON.stringify({ type: "session", id: session.id }));

    // Replay scrollback
    if (session.scrollback.length > 0) {
        ws.send(JSON.stringify({ type: "output", data: session.scrollback }));
    }

    ws.on("message", (msg: Buffer) => {
        try {
            const parsed = JSON.parse(msg.toString());
            if (parsed.type === "input" && session.alive) {
                session.pty.write(parsed.data);
            } else if (parsed.type === "resize" && session.alive) {
                session.pty.resize(parsed.cols, parsed.rows);
            }
        } catch {}
    });

    ws.on("close", () => {
        // Only clear if this ws is still the active one for this session
        if (session.ws === ws) {
            session.ws = null;
            if (session.alive) {
                startCleanupTimer(session);
            }
        }
    });
}

// --- REST API ---

app.get("/api/sessions", (_req, res) => {
    const ids = Array.from(sessions.values())
        .filter((s) => s.alive)
        .map((s) => s.id);
    res.json(ids);
});

// --- WebSocket handling ---

wss.on("connection", (ws: WebSocket) => {
    // Wait for the first message to decide: new session or attach
    let initialized = false;

    const initTimeout = setTimeout(() => {
        if (!initialized) {
            // No attach message received — create new session
            initialized = true;
            const session = createSession();
            attachWebSocket(session, ws);
        }
    }, 500);

    ws.on("message", (msg: Buffer) => {
        if (initialized) return; // already handled, let attachWebSocket handle further messages

        try {
            const parsed = JSON.parse(msg.toString());
            if (parsed.type === "attach" && parsed.sessionId) {
                initialized = true;
                clearTimeout(initTimeout);
                const session = sessions.get(parsed.sessionId);
                if (session && session.alive) {
                    attachWebSocket(session, ws);
                } else {
                    // Session gone — tell client to create a new one
                    ws.send(JSON.stringify({ type: "session_expired", sessionId: parsed.sessionId }));
                    // Create a fresh session instead
                    const newSession = createSession();
                    attachWebSocket(newSession, ws);
                }
            } else if (parsed.type === "new") {
                initialized = true;
                clearTimeout(initTimeout);
                const session = createSession();
                attachWebSocket(session, ws);
            }
        } catch {}
    });
});

const PORT = parseInt(process.env.PORT || "8090", 10);

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Terminal server running at http://0.0.0.0:${PORT}`);
});
