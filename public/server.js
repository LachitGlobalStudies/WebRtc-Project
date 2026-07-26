const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    cors: { 
        origin: "*",
        methods: ["GET", "POST"]
    },
    maxHttpBufferSize: 1e8, // 100MB for screen sharing
    pingTimeout: 60000,
    pingInterval: 25000
});

// Enable CORS for Render
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    next();
});

app.use(express.static('public'));

const roomTeachers = {};
const roomChatState = {};
const activeStreams = {};

// Store screen sharing sessions
const screenShareSessions = {};

io.on('connection', (socket) => {
    console.log('New connection:', socket.id);
    
    // Detect Android device
    const userAgent = socket.handshake.headers['user-agent'] || '';
    const isAndroid = /Android/i.test(userAgent);
    const isChrome = /Chrome/i.test(userAgent) && !/Edge/i.test(userAgent);
    
    if (isAndroid) {
        console.log('Android device connected:', socket.id);
        socket.emit('device-detected', { 
            isAndroid: true,
            isChrome: isChrome,
            screenShareSupported: true,
            message: 'Android screen sharing enabled'
        });
    }

    socket.on('join-room', ({ roomId, role }) => {
        socket.join(roomId);
        socket.role = role;
        socket.roomId = roomId;
        
        console.log(`${role} joined room: ${roomId}`);

        if (role === 'teacher') {
            roomTeachers[roomId] = socket.id;
            roomChatState[roomId] = false;
            socket.to(roomId).emit('teacher-connected', socket.id);
            // Initialize screen share session
            screenShareSessions[roomId] = { active: false };
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
                    otherUsers.push({ 
                        id: clientId, 
                        role: clientSocket ? clientSocket.role : 'student' 
                    });
                }
            }
        }

        socket.emit('all-users', { 
            users: otherUsers, 
            teacherId: roomTeachers[roomId],
            count: clients ? clients.size : 1
        });
        
        io.to(roomId).emit('user-joined', { 
            userId: socket.id, 
            role: role, 
            count: clients ? clients.size : 1 
        });

        // ==========================================
        // ANDROID SCREEN SHARING (Chrome compatible)
        // ==========================================
        
        // For Android: Receive screen share chunks
        socket.on('screen-share-chunk', (data) => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher' && currentRoom) {
                // Broadcast to all students in the room
                socket.to(currentRoom).emit('screen-share-chunk', {
                    chunk: data.chunk,
                    lastChunk: data.lastChunk || false,
                    timestamp: Date.now()
                });
            }
        });

        // Start screen share session
        socket.on('start-screen-share', () => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher' && currentRoom) {
                screenShareSessions[currentRoom] = { 
                    active: true,
                    startedAt: Date.now()
                };
                socket.to(currentRoom).emit('screen-share-started');
                socket.emit('screen-share-started');
                console.log(`Screen share started in room: ${currentRoom}`);
            }
        });

        // Stop screen share session
        socket.on('stop-screen-share', () => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher' && currentRoom) {
                screenShareSessions[currentRoom] = { active: false };
                socket.to(currentRoom).emit('screen-share-stopped');
                socket.emit('screen-share-stopped');
                console.log(`Screen share stopped in room: ${currentRoom}`);
            }
        });

        // ==========================================
        // WebRTC Signaling (for non-Android devices)
        // ==========================================
        
        socket.on('signal', (data) => {
            socket.to(data.target).emit('signal', {
                sender: socket.id,
                signal: data.signal
            });
        });

        // ==========================================
        // CHAT SYSTEM
        // ==========================================
        
        socket.on('send-message', (data) => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('receive-message', {
                sender: data.sender,
                text: data.text,
                timestamp: Date.now()
            });
        });

        socket.on('share-file', (data) => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('receive-file', {
                sender: data.sender,
                fileName: data.fileName,
                fileBuffer: data.fileBuffer,
                timestamp: Date.now()
            });
        });

        socket.on('toggle-room-chat', ({ open }) => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher') {
                roomChatState[currentRoom] = open;
                socket.to(currentRoom).emit('chat-state-changed', { open: open });
            }
        });

        // ==========================================
        // STUDENT MANAGEMENT
        // ==========================================
        
        socket.on('raise-hand', () => {
            const currentRoom = socket.roomId;
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) {
                io.to(teacherId).emit('student-raised-hand', socket.id);
            }
        });

        socket.on('allow-student', (studentId) => {
            const currentRoom = socket.roomId;
            io.to(studentId).emit('allowed-to-talk');
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) {
                io.to(teacherId).emit('single-student-unmuted-ui', studentId);
            }
        });

        socket.on('mute-student', (studentId) => {
            const currentRoom = socket.roomId;
            io.to(studentId).emit('force-mute');
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) {
                io.to(teacherId).emit('single-student-muted-ui', studentId);
            }
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

        // ==========================================
        // DISCONNECT HANDLING
        // ==========================================
        
        socket.on('disconnect', () => {
            console.log('Disconnected:', socket.id);
            const currentRoom = socket.roomId;
            
            if (socket.role === 'teacher') {
                if (screenShareSessions[currentRoom]) {
                    screenShareSessions[currentRoom] = { active: false };
                }
                delete roomTeachers[currentRoom];
                delete roomChatState[currentRoom];
                socket.to(currentRoom).emit('teacher-disconnected');
            } else {
                socket.to(currentRoom).emit('user-left', socket.id);
                const clients = io.sockets.adapter.rooms.get(currentRoom);
                io.to(currentRoom).emit('room-count-update', { 
                    count: clients ? clients.size : 0 
                });
            }
        });
    });
});

// Health check for Render
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Access at: http://localhost:${PORT}`);
});