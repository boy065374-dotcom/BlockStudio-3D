import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const server = http.createServer();
const wss = new WebSocketServer({ server });

const rooms = new Map();

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function broadcast(room, data, except = null) {
    for (const client of room.clients) {
        if (client !== except) send(client, data);
    }
}

wss.on('connection', (ws) => {
    ws.id = randomUUID();
    ws.roomId = null;

    send(ws, { type: 'connected', playerId: ws.id });

    ws.on('message', (raw) => {
        if (raw.length > 16384) return;

        let data;
        try {
            data = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (data.type === 'join') {
            const roomId = String(data.room || '').trim();

            if (!/^[a-zA-Z0-9_-]{3,32}$/.test(roomId)) {
                return send(ws, { type: 'error', message: 'Invalid room ID' });
            }

            if (ws.roomId) {
                const oldRoom = rooms.get(ws.roomId);
                if (oldRoom) {
                    oldRoom.clients.delete(ws);
                    broadcast(oldRoom, {
                        type: 'player-left',
                        playerId: ws.id
                    });
                    if (!oldRoom.clients.size) rooms.delete(ws.roomId);
                }
            }

            if (!rooms.has(roomId)) {
                rooms.set(roomId, { clients: new Set() });
            }

            const room = rooms.get(roomId);
            ws.roomId = roomId;

            const players = [...room.clients].map(client => ({
                playerId: client.id,
                state: client.state || null
            }));

            room.clients.add(ws);

            send(ws, {
                type: 'room-joined',
                room: roomId,
                players
            });

            broadcast(room, {
                type: 'player-joined',
                playerId: ws.id
            }, ws);

            return;
        }

        const room = rooms.get(ws.roomId);
        if (!room) return;

        if (data.type === 'state') {
            const { x, y, z, rotation } = data;

            if (![x, y, z, rotation].every(Number.isFinite)) return;

            ws.state = { x, y, z, rotation };

            broadcast(room, {
                type: 'player-update',
                playerId: ws.id,
                state: ws.state
            }, ws);
        }

        if (data.type === 'chat') {
            const message = String(data.message || '').trim().slice(0, 200);
            if (!message) return;

            broadcast(room, {
                type: 'chat',
                playerId: ws.id,
                message
            }, ws);
        }
    });

    ws.on('close', () => {
        const room = rooms.get(ws.roomId);
        if (!room) return;

        room.clients.delete(ws);

        broadcast(room, {
            type: 'player-left',
            playerId: ws.id
        });

        if (!room.clients.size) rooms.delete(ws.roomId);
    });
});

export default server;
