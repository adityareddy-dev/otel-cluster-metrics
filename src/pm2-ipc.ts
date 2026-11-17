import { connect, type Socket } from 'node:net';
import { assert } from './protocol.js';

/** Bounded newline framing for local IPC. JSON strings cannot contain literal newlines. */
export function readLines(socket:Socket,maxBytes:number,onLine:(line:string)=>void):void {
  let pending=Buffer.alloc(0);
  socket.on('data',(chunk:Buffer)=>{
    let start=0;
    for(let i=0;i<chunk.length;i++) if(chunk[i]===10) {
      if(pending.length+i-start>maxBytes) {socket.destroy(new Error('PM2 IPC frame exceeds byte limit'));return;}
      const line=Buffer.concat([pending,chunk.subarray(start,i)]).toString('utf8');pending=Buffer.alloc(0);start=i+1;
      try {onLine(line);} catch(error) {socket.destroy(error instanceof Error?error:new Error(String(error)));return;}
    }
    pending=Buffer.concat([pending,chunk.subarray(start)]);
    if(pending.length>maxBytes) socket.destroy(new Error('PM2 IPC frame exceeds byte limit'));
  });
}

/** A persistent pipe also identifies the lifetime of an old worker during PM2 reload. */
export class Pm2WorkerConnection {
  private readonly socket:Socket;
  private readonly ready:Promise<void>;
  private failure?:Error;
  constructor(socketPath:string,maxBytes:number,hello:Record<string,unknown>,onMessage:(raw:unknown)=>void) {
    assert(typeof socketPath==='string' && socketPath.length>0,'Supply pm2SocketPath for the PM2 reader');
    this.socket=connect(socketPath);
    this.ready=new Promise((resolve,reject)=>{
      this.socket.once('connect',()=>{this.socket.write(JSON.stringify(hello)+'\n');resolve();});
      this.socket.once('error',reject);
    });
    // Connection errors surface on flush/shutdown. Merely constructing a reader must not reject unobserved.
    void this.ready.catch(()=>{});
    this.socket.on('error',error=>{this.failure=error;});
    this.socket.on('close',()=>{this.failure ??=new Error('PM2 collector socket closed; restart app instances with the collector');});
    readLines(this.socket,maxBytes,onMessage);
  }
  async write(body:string):Promise<void> {
    await this.ready;
    if(this.failure) throw this.failure;
    await new Promise<void>((resolve,reject)=>this.socket.write(body+'\n',error=>error?reject(error):resolve()));
  }
  close():void {this.socket.destroy();}
}
