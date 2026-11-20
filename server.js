const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');

const app = express();
const http = require('http').createServer(app);

app.use(cors({ origin: "*" }));
app.use(express.static('public'));

const io = require('socket.io')(http, {
    cors: { 
        origin: "*",
        methods: ["GET", "POST"]
    },
    maxHttpBufferSize: 1e7, // 10MB Limit
    pingTimeout: 60000,     // ৬০ সেকেন্ড পর্যন্ত ওয়েট করবে (Render-এর জন্য জরুরি)
    pingInterval: 25000     // প্রতি ২৫ সেকেন্ডে কানেকশন চেক করবে
});

// উপরে ভেরিয়েবল ডিক্লেয়ারেশনের জায়গায় এটি যোগ করুন:
const teacherDisconnectTimers = {};

app.get('/get-html-page', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// In-Memory State
const roomTeachers = {};
const roomChatState = {}; 
const activeStreams = {};
const pendingStudents = {}; // { roomId: [ { id, name } ] }
const approvedStudents = {}; // { roomId: Set([studentName]) } -> ট্র্যাক রাখার জন্য
const socketUserData = {};  // { [socket.id]: { role, roomId, name } }

io.on('connection', (socket) => {
    // Keep-Alive Handler (Render & Mobile NAT timeout প্রতিরোধ করবে)
    socket.on('keep-alive', () => {
        socket.emit('keep-alive-ack');
    });
    // Teacher Joins
    socket.on('join-room', ({ roomId, role }) => {
        socket.join(roomId);
        socket.role = role;
        socket.roomId = roomId; 
        socketUserData[socket.id] = { role, roomId, name: role === 'teacher' ? 'Teacher' : 'Student' };

        if (role === 'teacher') {
            // যদি টিচার ১০ সেকেন্ডের মধ্যে রিকানেক্ট করে, তবে ডিসকানেক্ট টাইমার বাতিল করা
            if (teacherDisconnectTimers[roomId]) {
                clearTimeout(teacherDisconnectTimers[roomId]);
                delete teacherDisconnectTimers[roomId];
            }

            roomTeachers[roomId] = socket.id;
            if (roomChatState[roomId] === undefined) roomChatState[roomId] = false; 
            if (!pendingStudents[roomId]) pendingStudents[roomId] = [];
            if (!approvedStudents[roomId]) approvedStudents[roomId] = new Set();
            
            socket.to(roomId).emit('teacher-connected', socket.id);
            socket.emit('pending-list-update', pendingStudents[roomId]);
        }

        const clients = io.sockets.adapter.rooms.get(roomId);
        const otherUsers = [];
        if (clients) {
            for (let clientId of clients) {
                if (clientId !== socket.id) {
                    const clientData = socketUserData[clientId];
                    otherUsers.push({ 
                        id: clientId, 
                        role: clientData ? clientData.role : 'student',
                        name: clientData ? clientData.name : 'Student'
                    });
                }
            }
        }

        socket.emit('all-users', { users: otherUsers, teacherId: roomTeachers[roomId] });
        io.to(roomId).emit('user-joined', { 
            userId: socket.id, 
            role: role, 
            name: socketUserData[socket.id].name,
            count: clients ? clients.size : 1 
        });
    });

    // Student Requests Admission
    socket.on('request-join', ({ roomId, studentName }) => {
        const name = (studentName && studentName.trim()) ? studentName.trim() : `Student (${socket.id.substring(0,4)})`;
        socket.role = 'student';
        socket.roomId = roomId;
        socket.studentName = name;
        socketUserData[socket.id] = { role: 'student', roomId, name };

        const teacherId = roomTeachers[roomId];
        if (!teacherId) {
            socket.emit('join-rejected', { reason: 'Teacher is not in this class yet. Please wait or check your Room ID.' });
            return;
        }

        if (!pendingStudents[roomId]) pendingStudents[roomId] = [];
        // ডুপ্লিকেট রিকোয়েস্ট রিমুভ করা
        pendingStudents[roomId] = pendingStudents[roomId].filter(s => s.name !== name);
        pendingStudents[roomId].push({ id: socket.id, name });

        socket.emit('join-pending');
        io.to(teacherId).emit('student-join-request', { id: socket.id, name });
        io.to(teacherId).emit('pending-list-update', pendingStudents[roomId]);
    });

    // Auto-Rejoin for Previously Accepted Students
    socket.on('auto-rejoin', ({ roomId, studentName }) => {
        const teacherId = roomTeachers[roomId];
        if (!teacherId) {
            socket.emit('waiting-for-teacher-reconnect');
            return;
        }

        const name = (studentName && studentName.trim()) ? studentName.trim() : `Student (${socket.id.substring(0,4)})`;
        socket.role = 'student';
        socket.roomId = roomId;
        socket.studentName = name;
        socketUserData[socket.id] = { role: 'student', roomId, name };

        socket.join(roomId);
        socket.emit('join-accepted', { 
            teacherId: teacherId,
            roomId: roomId,
            isReconnect: true
        });

        if (roomChatState[roomId] === true) {
            socket.emit('chat-state-changed', { open: true });
        }

        const clients = io.sockets.adapter.rooms.get(roomId);
        io.to(roomId).emit('user-joined', { 
            userId: socket.id, 
            role: 'student', 
            name: name,
            count: clients ? clients.size : 1 
        });
    });

    // Teacher Approves Student
    socket.on('accept-student', ({ studentId }) => {
        const currentRoom = socket.roomId;
        if (socket.role !== 'teacher') return;

        if (pendingStudents[currentRoom]) {
            pendingStudents[currentRoom] = pendingStudents[currentRoom].filter(s => s.id !== studentId);
            socket.emit('pending-list-update', pendingStudents[currentRoom]);
        }

        const studentSocket = io.sockets.sockets.get(studentId);
        if (studentSocket) {
            studentSocket.join(currentRoom);
            const studentInfo = socketUserData[studentId] || { name: 'Student' };
            
            if (!approvedStudents[currentRoom]) approvedStudents[currentRoom] = new Set();
            approvedStudents[currentRoom].add(studentInfo.name);

            studentSocket.emit('join-accepted', { 
                teacherId: roomTeachers[currentRoom],
                roomId: currentRoom,
                isReconnect: false
            });

            if (roomChatState[currentRoom] === true) {
                studentSocket.emit('chat-state-changed', { open: true });
            }

            const clients = io.sockets.adapter.rooms.get(currentRoom);
            io.to(currentRoom).emit('user-joined', { 
                userId: studentId, 
                role: 'student', 
                name: studentInfo.name,
                count: clients ? clients.size : 1 
            });
        }
    });

    // Student Signals they are ready with camera & mic
    socket.on('student-ready-for-call', ({ teacherId }) => {
        if (teacherId) {
            io.to(teacherId).emit('start-call-with-student', { 
                studentId: socket.id,
                name: socketUserData[socket.id] ? socketUserData[socket.id].name : 'Student'
            });
        }
    });

    // Teacher Rejects Student
    socket.on('reject-student', ({ studentId }) => {
        const currentRoom = socket.roomId;
        if (socket.role !== 'teacher') return;

        if (pendingStudents[currentRoom]) {
            pendingStudents[currentRoom] = pendingStudents[currentRoom].filter(s => s.id !== studentId);
            socket.emit('pending-list-update', pendingStudents[currentRoom]);
        }

        io.to(studentId).emit('join-rejected', { reason: 'The teacher declined your request to join the class.' });
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

        ffmpeg.on('close', () => {
            delete activeStreams[currentRoom];
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
        const studentName = socketUserData[socket.id] ? socketUserData[socket.id].name : 'A student';
        if (teacherId) {
            io.to(teacherId).emit('student-raised-hand', { 
                studentId: socket.id, 
                name: studentName 
            });
        }
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

    // Add this right here inside io.on('connection') in server.js:
    socket.on('refresh-all-students', ({ roomId }) => {
        if (socket.role !== 'teacher') return;
        const targetRoom = roomId || socket.roomId;
        socket.to(targetRoom).emit('', { 
            teacherId: socket.id 
        });
    });

    // Disconnect Handling
    socket.on('disconnect', () => {
        const currentRoom = socket.roomId;
        const socketId = socket.id;

        if (currentRoom && pendingStudents[currentRoom]) {
            pendingStudents[currentRoom] = pendingStudents[currentRoom].filter(s => s.id !== socketId);
            const teacherId = roomTeachers[currentRoom];
            if (teacherId) io.to(teacherId).emit('pending-list-update', pendingStudents[currentRoom]);
        }

        if (socketUserData[socketId]) {
            delete socketUserData[socketId];
        }

        if (!currentRoom) return;

        if (socket.role === 'teacher') {
            // সাথে সাথে ডিসকানেক্ট না দেখিয়ে ১২ সেকেন্ড সময় দেওয়া (Auto-reconnect এর জন্য)
            teacherDisconnectTimers[currentRoom] = setTimeout(() => {
                if (roomTeachers[currentRoom] === socketId) {
                    if (activeStreams[currentRoom]) {
                        activeStreams[currentRoom].stdin.end();
                        delete activeStreams[currentRoom];
                    }
                    delete roomTeachers[currentRoom];
                    delete roomChatState[currentRoom];
                    delete pendingStudents[currentRoom];
                    io.to(currentRoom).emit('teacher-disconnected');
                }
                delete teacherDisconnectTimers[currentRoom];
            }, 12000);
        } else {
            socket.to(currentRoom).emit('user-left', socket.id);
            const clients = io.sockets.adapter.rooms.get(currentRoom);
            io.to(currentRoom).emit('room-count-update', { count: clients ? clients.size : 0 });
        }
    });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
