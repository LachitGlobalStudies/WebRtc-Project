const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');

const app = express();
const http = require('http').createServer(app);

// Express and Middleware Setup
app.use(cors({ origin: "*" }));
app.use(express.static('public'));

const io = require('socket.io')(http, {
    cors: { 
        origin: "*",
        methods: ["GET", "POST"]
    },
    maxHttpBufferSize: 1e7 // 10MB Limit
});

// Routes
app.get('/get-html-page', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// In-Memory State
const roomTeachers = {};
const roomChatState = {}; 
const activeStreams = {};

// Socket.io Connection Logic
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
        io.to(roomId).emit('user-joined', { userId: socket.id, role: role, count: clients ? clients.size : 1 });
    });

    // WebRTC Signaling
    socket.on('signal', (data) => {
        socket.to(data.target).emit('signal', {
            sender: socket.id,
            signal: data.signal
        });
    });

    // YouTube RTMP Live Streaming (FFmpeg)
    socket.on('start-youtube-stream', ({ streamKey }) => {
        if (socket.role !== 'teacher') return;
        const currentRoom = socket.roomId;

        if (activeStreams[currentRoom]) {
            return socket.emit('stream-error', 'Stream already running!');
        }

        const youtubeUrl = `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
        
        const ffmpeg = spawn('ffmpeg', [
            '-i', '-',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-b:v', '2500k',
            '-maxrate', '2500k',
            '-bufsize', '5000k',
            '-pix_fmt', 'yuv420p',
            '-g', '50',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-ar', '44100',
            '-f', 'flv',
            youtubeUrl
        ]);

        ffmpeg.on('close', (code) => {
            console.log(`FFmpeg process closed with code ${code}`);
            delete activeStreams[currentRoom];
        });

        ffmpeg.stderr.on('data', (data) => {
            console.log('FFmpeg Log:', data.toString());
        });

        activeStreams[currentRoom] = ffmpeg;
        socket.emit('stream-started');
    });

    socket.on('stream-chunk', (chunk) => {
        const currentRoom = socket.roomId;
        if (activeStreams[currentRoom]) {
            activeStreams[currentRoom].stdin.write(chunk);
        }
    });

    socket.on('stop-youtube-stream', () => {
        const currentRoom = socket.roomId;
        if (activeStreams[currentRoom]) {
            activeStreams[currentRoom].stdin.end();
            delete activeStreams[currentRoom];
            socket.emit('stream-stopped');
        }
    });

    // Chat and File Sharing
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

    // Classroom Controls and Moderation
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

    socket.on('switch-student-camera', ({ targetStudentId }) => {
        io.to(targetStudentId).emit('request-camera-switch');
    });

    // Disconnect Handling
    socket.on('disconnect', () => {
        const currentRoom = socket.roomId;
        if (!currentRoom) return;

        if (socket.role === 'teacher') {
            if (activeStreams[currentRoom]) {
                activeStreams[currentRoom].stdin.end();
                delete activeStreams[currentRoom];
            }
            delete roomTeachers[currentRoom];
            delete roomChatState[currentRoom];
            socket.to(currentRoom).emit('teacher-disconnected');
        } else {
            socket.to(currentRoom).emit('user-left', socket.id);
            
            const clients = io.sockets.adapter.rooms.get(currentRoom);
            io.to(currentRoom).emit('room-count-update', { count: clients ? clients.size : 0 });
        }
    });
});

// Server Initialization
const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});