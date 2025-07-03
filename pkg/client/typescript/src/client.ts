
let createWebSocket: (url: string) => WebSocket;

if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
    // Browser environment
    createWebSocket = (url: string) => new WebSocket(url);
} else {
    // Node.js
    const ws = require("ws");
    createWebSocket = (url: string) => new ws(url);
}



type Message = {
    id: string;
    type: string;
    payload: any;
};

const socket = createWebSocket("ws://127.0.0.1:9001");

socket.onopen = () => {
    const authMessage: Message = {
        id: "auth-1",
        type: "Auth",
        payload: { access_token: "mysecrettoken" },
    };

    socket.send(JSON.stringify(authMessage));

    setTimeout(() => {
        const query: Message = {
            id: "req-1",
            type: "Query",
            payload: { statement: "from trades" },
        };
        socket.send(JSON.stringify(query));
    }, 200);
};

socket.onmessage = (event) => {
    const data = JSON.parse(event.data as string);
    console.log("Received:", data);
};
