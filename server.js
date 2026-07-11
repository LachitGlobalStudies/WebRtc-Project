const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    cors: { origin: "*" }
});

app.use(express.static('public'));

// কোন রুমে শিক্ষকের socket.id কত, তা ট্র্যাক করার জন্য
const roomTeachers = {};

io.on('connection', (socket) => {
    
    socket.on('join-room', ({ roomId, role }) => {
        socket.join(roomId);
        socket.role = role; // 'teacher' অথবা 'student'
        socket.roomId = roomId;

        if (role === 'teacher') {
            roomTeachers[roomId] = socket.id;
            // শিক্ষক জয়েন করলে রুমে থাকা সবাইকে অ্যালার্ট দেওয়া
            socket.to(roomId).emit('teacher-connected', socket.id);
        }

        // রুমে ইতিমধ্যে যারা আছে তাদের লিস্ট তৈরি করা
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

        // নতুন জয়েন করা ইউজারকে বর্তমান রুমের ডেটা পাঠানো
        socket.emit('all-users', { users: otherUsers, teacherId: roomTeachers[roomId] });

        // বাকিদের জানানো যে নতুন একজন এসেছে
        socket.to(roomId).emit('user-joined', { userId: socket.id, role: role });

        // WebRTC Signaling (Offer, Answer, ICE Candidates)
        socket.on('signal', (data) => {
            socket.to(data.target).emit('signal', {
                sender: socket.id,
                signal: data.signal
            });
        });

        // স্টুডেন্ট হাত তুললে শুধু শিক্ষককে জানানো
        socket.on('raise-hand', () => {
            const teacherId = roomTeachers[roomId];
            if (teacherId) {
                io.to(teacherId).emit('student-raised-hand', socket.id);
            }
        });

        // শিক্ষক অনুমতি দিলে নির্দিষ্ট স্টুডেন্টকে জানানো
        socket.on('allow-student', (studentId) => {
            io.to(studentId).emit('allowed-to-talk');
        });

        socket.on('disconnect', () => {
            if (role === 'teacher') {
                delete roomTeachers[roomId];
                socket.to(roomId).emit('teacher-disconnected');
            } else {
                socket.to(roomId).emit('user-left', socket.id);
            }
        });
    });
});

http.listen(3000, () => {
    console.log('Server runs on http://localhost:3000');
});