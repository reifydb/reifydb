import WebSocket from "ws"; // For Node.js; in browser, use native WebSocket
import { v4 as uuidv4 } from "uuid";

type Message = {
    id: string;
    type: "query" | "result" | "error" | string;
    payload: any;
};

const socket = new WebSocket("ws://127.0.0.1:9001");

socket.onopen = () => {
    console.log("Connected to ReifyDB WebSocket server");

    const query: Message = {
        id: "req1",
        type: "query",
        payload: {
            statement: "from trades",
        },
    };

    socket.send(JSON.stringify(query));
};

socket.onmessage = (event) => {
    try {
        const message: Message = JSON.parse(event.data.toString());
        console.log("Received:", message);
    } catch (err) {
        console.error("Invalid message format:", err);
    }
};

socket.onerror = (err) => {
    console.error("WebSocket error:", err);
};

socket.onclose = () => {
    console.log("WebSocket connection closed.");
};
