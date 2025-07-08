import {WsClient} from "./ws";

async function main() {
    const client = await WsClient.connect("ws://127.0.0.1:9001");


    const frames = await client.tx<[
        { abc: number },
        { dec: number }
    ]>("SELECT cast(127, int1) as abc; SELECT 2 as dec;");

    const frame0 = frames[0];
    const frame1 = frames[1];

    console.log(frame0[0].abc);
    console.log(frame1[0].dec);
}


main();