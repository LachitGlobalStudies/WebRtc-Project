const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    cors: { origin: "*" },
    maxHttpBufferSize: 1e7 // 10MB Limit
});

app.use(express.static('public'));

const roomTeachers = {};
const roomChatState = {}; 

io.on('connection', (socket) => {
    
    socket.on('join-room', ({ roomId, role }) => {
        socket.join(roomId);
        socket.role = role;
        socket.roomId = roomId; 

        if (role === 'teacher') {
            roomTeachers[roomId] = socket.id;
            roomChatState[roomId] = false; 
            socket.to(roomId).emit('teacher-connected', socket.id);
        }

        if (role === 'student' && roomChatState[roomId] === true) {
            socket.emit('chat-state-changed', { open: true });
        }

        const clients = io.sockets.adapter.rooms.get(roomId);
        const otherUsers = [];
        if (clients) {
            for (let clientId of clients) {
                if (clientId !== socket.id) {
                    const clientSocket = io.sockets.sockets.get(clientId);
                    otherUsers.push({ id: clientId, role: clientSocket ? clientSocket.role : 'student' });
                }
            }
        }

        socket.emit('all-users', { users: otherUsers, teacherId: roomTeachers[roomId] });
        socket.to(roomId).emit('user-joined', { userId: socket.id, role: role });

        socket.on('signal', (data) => {
            socket.to(data.target).emit('signal', {
                sender: socket.id,
                signal: data.signal
            });
        });

        // --- Realtime Whiteboard Draw Transmission ---
        socket.on('draw-data', (data) => {
            const currentRoom = socket.roomId;
            // Teacher-er drawing data shob student-der kache forward hobe
            socket.to(currentRoom).emit('incoming-draw', data);
        });

        // --- Whiteboard Clear Action ---
        socket.on('clear-board', () => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('incoming-clear');
        });

        socket.on('toggle-room-chat', ({ open }) => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher') {
                roomChatState[currentRoom] = open;
                socket.to(currentRoom).emit('chat-state-changed', { open: open });
            }
        });

        socket.on('send-message', (data) => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('receive-message', {
                sender: data.sender,
                text: data.text
            });
        });

        socket.on('share-file', (data) => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('receive-file', {
                sender: data.sender,
                fileName: data.fileName,
                fileBuffer: data.fileBuffer
            });
        });

        socket.on('raise-hand', () => {
            const currentRoom = socket.roomId;
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) io.to(teacherId).emit('student-raised-hand', socket.id);
        });

        socket.on('allow-student', (studentId) => {
            const currentRoom = socket.roomId;
            io.to(studentId).emit('allowed-to-talk');
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) io.to(teacherId).emit('single-student-unmuted-ui', studentId);
        });

        socket.on('mute-student', (studentId) => {
            const currentRoom = socket.roomId;
            io.to(studentId).emit('force-mute');
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) io.to(teacherId).emit('single-student-muted-ui', studentId);
        });

        socket.on('mute-all-students', () => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('force-mute');
            socket.emit('all-students-muted-ui');
        });

        socket.on('unmute-all-students', () => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('allowed-to-talk');
            socket.emit('all-students-unmuted-ui');
        });

        socket.on('disconnect', () => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher') {
                delete roomTeachers[currentRoom];
                delete roomChatState[currentRoom];
                socket.to(currentRoom).emit('teacher-disconnected');
            } else {
                socket.to(currentRoom).emit('user-left', socket.id);
            }
        });
    });
});

http.listen(3000, () => {
    console.log('Server runs on http://localhost:3000');
});