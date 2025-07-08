// let createWebSocket: (url: string) => WebSocket;
//
// if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
//     // Browser environment
//     createWebSocket = (url: string) => new WebSocket(url);
// } else {
//     // Node.js
//     const ws = require("ws");
//     createWebSocket = (url: string) => new ws(url);
// }
//
//
// type Message = {
//     id: string;
//     type: string;
//     payload: any;
// };
//
// const socket = createWebSocket("ws://127.0.0.1:9001");
//
// socket.onopen = () => {
//     const authMessage: Message = {
//         id: "auth-1",
//         type: "Auth",
//         payload: {token: "mysecrettoken", id: "test"},
//     };
//
//     socket.send(JSON.stringify(authMessage));
//
//     setTimeout(() => {
//         const query: Message = {
//             id: "req-1",
//             type: "Query",
//             payload: {statements: ["from trades"], id: "test"},
//         };
//         socket.send(JSON.stringify(query));
//     }, 200);
// };
//
// socket.onmessage = (event) => {
//     const data = JSON.parse(event.data as string);
//     console.log("Received:", JSON.stringify(data, null, 2));
// };


import {ReifyClient} from "./websocket";


async function main() {
    const client = await ReifyClient.connect("ws://127.0.0.1:9001");


    const frames = await client.execute<[
        { abc: number },
        { dec: number }
    ]>("SELECT cast(129, int1) as abc; SELECT 2 as dec;");

    const frame0 = frames[0];
    const frame1 = frames[1];

    console.log(frame0[0].abc);
    console.log(frame1[0].dec);
}


main();