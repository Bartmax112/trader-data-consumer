const express = require('express');
const app = express();
const path = require('path');
const kafka = require('kafka-node');

const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

const kafkaHost = 'kafka-1:9092';
const topic = 'gold';

// Function to connect and start consuming messages
const connectAndConsume = () => {
    const client = new kafka.KafkaClient({ kafkaHost });
    const consumer = new kafka.Consumer(client, [{ topic }]);

    consumer.on('message', function (message) {
        io.emit('message', message);
    });

    consumer.on('error', function (error) {
        console.error('Kafka consumer error:', error);
        // Retry connection after a delay
        setTimeout(connectAndConsume, 5000); // Retry after 5 seconds
    });
};

// Start consuming messages
connectAndConsume();

// Route to render index.ejs
app.get('/', (req, res) => {
    res.render('index');
});

// Start the server
const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';
http.listen(PORT, HOST, () => {
    console.log(`Server is running on port ${PORT}`);
});
