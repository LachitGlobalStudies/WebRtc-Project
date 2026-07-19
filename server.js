const express = require('express');
const app = express();
const http = require('http').createServer(app);
const { spawn } = require('child_process'); // Required for streaming to YouTube
const io = require('socket.io')(http, {
    cors: { origin: "*" },
    maxHttpBufferSize: 1e7 // 10MB Limit
});

app.use(express.static('public'));

const roomTeachers = {};
const roomChatState = {}; 
const activeStreams = {}; // Tracks active FFmpeg processes per room

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

        // Send all users to the newly joined client
        socket.emit('all-users', { users: otherUsers, teacherId: roomTeachers[roomId] });
        
        // Notify others and force an update of the total participant count
        io.to(roomId).emit('user-joined', { userId: socket.id, role: role, count: clients ? clients.size : 1 });

        socket.on('signal', (data) => {
            socket.to(data.target).emit('signal', {
                sender: socket.id,
                signal: data.signal
            });
        });

        // ==========================================
        // 🚀 NEW: Android Native Screen Sharing Channel
        // ==========================================
        socket.on('screenDataChunk', (chunk) => {
            const currentRoom = socket.roomId;
            if (socket.role === 'teacher' && currentRoom) {
                // App theke asha raw byte chunk shorashori room-er student-der kache broadcast hobe
                socket.to(currentRoom).emit('streamToStudent', chunk);
            }
        });

        // --- YouTube Livestream Event Channels ---
        socket.on('start-youtube-stream', ({ streamKey }) => {
            if (socket.role !== 'teacher') return;
            const currentRoom = socket.roomId;

            if (activeStreams[currentRoom]) {
                return socket.emit('stream-error', 'Stream already running!');
            }

            const youtubeUrl = `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
            
            // Spawn FFmpeg to convert incoming WebM chunks into a standard RTMP FLV format for YouTube
            const ffmpeg = spawn('ffmpeg', [
                '-i', '-',                // Read input from standard input (stdin)
                '-c:v', 'libx264',        // Encode video to H.264
                '-preset', 'veryfast',    // Encoding speed profile
                '-b:v', '2500k',          // Target video bitrate
                '-maxrate', '2500k',
                '-bufsize', '5000k',
                '-pix_fmt', 'yuv420p',    // Color space requirement for modern video players
                '-g', '50',               // Keyframe interval (2-second interval at 25fps)
                '-c:a', 'aac',            // Encode audio to AAC
                '-b:a', '128k',           // Audio bitrate
                '-ar', '44100',           // Audio sample rate
                '-f', 'flv',              // YouTube accepts FLV over RTMP
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

        // --- Realtime Whiteboard Draw Transmission ---
        socket.on('draw-data', (data) => {
            const currentRoom = socket.roomId;
            socket.to(currentRoom).emit('incoming-draw', data);
        });

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
});

http.listen(3000, () => {
    console.log('Server runs on http://localhost:3000');
});
