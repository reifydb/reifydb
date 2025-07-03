import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import {TxRequest} from "./generated/reifydb";

const packageDefinition = protoLoader.loadSync('proto/reifydb.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const client = new proto.grpc_db.DB('localhost:54321', grpc.credentials.createInsecure());

const metadata = new grpc.Metadata();
metadata.add('authorization', 'Bearer mysecrettoken');

const request: TxRequest = {
    query: 'from test.arith'
};

const stream = client.Tx(request, metadata);

stream.on('data', (result: any) => {
    console.log('Received:', JSON.stringify(result, null, 2));
});

stream.on('error', (err: any) => {
    console.error('Error:', err);
});

stream.on('end', () => {
    console.log('Stream ended.');
});

console.log("done");