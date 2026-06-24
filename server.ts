import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import * as pty from "node-pty";
import path from "path";
import fs from "fs";
import os from "os";
import { execSync } from "child_process";
import crypto from "crypto";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const publicDir = fs.existsSync(path.join(__dirname, "public"))
    ? path.join(__dirname, "public")
    : path.join(__dirname, "..", "public");

app.use(express.json());
app.use(express.static(publicDir));

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

// Some shells (zsh on Linux, bash without VTE) don't emit OSC 7 by default.
// We inject a hook so the pty always reports cwd changes, regardless of the
// user's dotfiles. macOS zsh already does this via /etc/zshrc.
function setupZshOsc7Dir(): string | null {
    try {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), "web-terminal-zsh-"));
        const home = process.env.HOME || "";
        const shq = (p: string) => "'" + p.replace(/'/g, "'\\''") + "'";

        for (const file of [".zshenv", ".zprofile", ".zlogin"]) {
            const real = path.join(home, file);
            fs.writeFileSync(
                path.join(dir, file),
                `[[ -f ${shq(real)} ]] && source ${shq(real)}\n`,
            );
        }

        const realZshrc = path.join(home, ".zshrc");
        fs.writeFileSync(
            path.join(dir, ".zshrc"),
            `[[ -f ${shq(realZshrc)} ]] && source ${shq(realZshrc)}\n` +
                `_web_terminal_emit_cwd() { printf '\\e]7;file://%s%s\\e\\\\' "\${HOST}" "\${PWD}" }\n` +
                `autoload -Uz add-zsh-hook 2>/dev/null && add-zsh-hook precmd _web_terminal_emit_cwd\n`,
        );
        return dir;
    } catch (err) {
        console.error("Failed to set up zsh OSC 7 hook dir:", err);
        return null;
    }
}

const shellBase = path.basename(shell);
const zshHookDir = shellBase === "zsh" ? setupZshOsc7Dir() : null;
const bashOsc7 =
    shellBase === "bash"
        ? `printf '\\033]7;file://%s%s\\033\\\\' "\${HOSTNAME}" "\${PWD}"`
        : null;

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
    cwd: string;
}

// OSC 7 sequence: ESC ] 7 ; file://host/path BEL (or ESC ] 7 ; ... ESC \)
const OSC7_REGEX = /\x1b\]7;file:\/\/[^/]*(\/[^\x07\x1b]*)(?:\x07|\x1b\\)/g;

function decodeCwd(encoded: string): string {
    try {
        return decodeURIComponent(encoded);
    } catch {
        return encoded;
    }
}

const HOME = process.env.HOME || "";

function displayCwd(cwd: string): string {
    if (HOME && (cwd === HOME || cwd.startsWith(HOME + "/"))) {
        return "~" + cwd.slice(HOME.length);
    }
    return cwd;
}

const sessions = new Map<string, Session>();

function createSession(): Session {
    const id = crypto.randomUUID();
    const initialCwd = process.env.HOME || "/";
    const env: Record<string, string> = { ...(process.env as Record<string, string>) };
    if (zshHookDir) {
        env.ZDOTDIR = zshHookDir;
    }
    if (bashOsc7) {
        env.PROMPT_COMMAND = env.PROMPT_COMMAND
            ? `${bashOsc7};${env.PROMPT_COMMAND}`
            : bashOsc7;
    }
    const term = pty.spawn(shell, [], {
        name: "xterm-256color",
        cols: 80,
        rows: 24,
        cwd: initialCwd,
        env,
    });

    const session: Session = {
        id,
        pty: term,
        scrollback: "",
        ws: null,
        cleanupTimer: null,
        alive: true,
        cwd: initialCwd,
    };

    term.onData((data: string) => {
        // Buffer scrollback
        session.scrollback += data;
        if (session.scrollback.length > MAX_SCROLLBACK) {
            session.scrollback = session.scrollback.slice(-MAX_SCROLLBACK);
        }
        // Detect OSC 7 cwd updates
        let match;
        OSC7_REGEX.lastIndex = 0;
        let lastCwd: string | null = null;
        while ((match = OSC7_REGEX.exec(data)) !== null) {
            lastCwd = decodeCwd(match[1]);
        }
        if (lastCwd && lastCwd !== session.cwd) {
            session.cwd = lastCwd;
            if (session.ws && session.ws.readyState === WebSocket.OPEN) {
                session.ws.send(JSON.stringify({ type: "cwd", cwd: displayCwd(lastCwd) }));
            }
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

    // Send current cwd
    ws.send(JSON.stringify({ type: "cwd", cwd: displayCwd(session.cwd) }));

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

// Upload a dropped file into the session's current working directory.
app.post(
    "/api/upload",
    express.raw({ type: "application/octet-stream", limit: "100mb" }),
    (req, res) => {
        const sessionId = String(req.query.sessionId || "");
        const session = sessions.get(sessionId);
        if (!session || !session.alive) {
            res.status(404).json({ error: "session not found" });
            return;
        }
        // Strip any path components to prevent traversal outside cwd.
        const filename = path.basename(String(req.query.filename || ""));
        if (!filename || filename === "." || filename === "..") {
            res.status(400).json({ error: "invalid filename" });
            return;
        }
        const body = req.body;
        if (!Buffer.isBuffer(body) || body.length === 0) {
            res.status(400).json({ error: "empty body" });
            return;
        }
        const dest = path.join(session.cwd, filename);
        try {
            fs.writeFileSync(dest, body);
            res.json({ path: dest, displayPath: displayCwd(dest) });
        } catch (err) {
            console.error("Upload failed:", err);
            res.status(500).json({ error: "write failed" });
        }
    },
);

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
