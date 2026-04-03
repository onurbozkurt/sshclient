import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import * as pty from "node-pty";
import path from "path";
import { execSync } from "child_process";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

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

wss.on("connection", (ws: WebSocket) => {
  let term: pty.IPty;
  try {
    term = pty.spawn(shell, [], {
      name: "xterm-256color",
      cols: 80,
      rows: 24,
      cwd: process.env.HOME || "/",
      env: process.env as Record<string, string>,
    });
  } catch (err) {
    console.error("Failed to spawn shell:", err);
    ws.send(JSON.stringify({ type: "output", data: `\r\nError: Failed to spawn shell (${shell})\r\n${err}\r\n` }));
    ws.close();
    return;
  }

  term.onData((data: string) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "output", data }));
    }
  });

  term.onExit(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "exit" }));
    }
  });

  ws.on("message", (msg: Buffer) => {
    try {
      const parsed = JSON.parse(msg.toString());
      if (parsed.type === "input") {
        term.write(parsed.data);
      } else if (parsed.type === "resize") {
        term.resize(parsed.cols, parsed.rows);
      }
    } catch {}
  });

  ws.on("close", () => {
    term.kill();
  });
});

const PORT = parseInt(process.env.PORT || "3000", 10);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Terminal server running at http://0.0.0.0:${PORT}`);
});
