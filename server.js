const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    cors: { origin: "*" }
});

app.use(express.static('public'));

io.on('connection', (socket) => {
    
    socket.on('join-room', (roomId) => {
        socket.join(roomId);

        // রুমে ইতিমধ্যে যারা আছে তাদের আইডি নতুন জয়েন করা ইউজারের কাছে পাঠানো
        const clients = io.sockets.adapter.rooms.get(roomId);
        const otherUsers = Array.from(clients).filter(id => id !== socket.id);
        socket.emit('all-users', otherUsers);

        // রুমে থাকা বাকিদের জানানো যে নতুন একজন এসেছে
        socket.to(roomId).emit('user-joined', socket.id);

        // নির্দিষ্ট ইউজারকে লক্ষ্য করে সিগনালিং ডেটা পাস করা
        socket.on('signal', (data) => {
            socket.to(data.target).emit('signal', {
                sender: socket.id,
                signal: data.signal
            });
        });

        socket.on('disconnect', () => {
            socket.to(roomId).emit('user-left', socket.id);
        });
    });
});

http.listen(3000, () => {
    console.log('Server runs on http://localhost:3000');
});