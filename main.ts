const USER_ID = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d";
const WS_PATH = "/myvless";

function parseUUID(buffer: Uint8Array, offset: number): string {
  const bytes = buffer.slice(offset, offset + 16);
  const hex = Array.from(bytes).map((b) => b.toString(16).padStart(2, "0"));
  return `${hex.slice(0,4).join("")}-${hex.slice(4,6).join("")}-${hex.slice(6,8).join("")}-${hex.slice(8,10).join("")}-${hex.slice(10,16).join("")}`;
}

Deno.serve(async (req) => {
  const url = new URL(req.url);

  if (url.pathname !== WS_PATH || req.headers.get("upgrade") !== "websocket") {
    return new Response("Not found", { status: 404 });
  }

  const { socket, response } = Deno.upgradeWebSocket(req);

  let remoteConn: Deno.TcpConn | null = null;

  socket.onmessage = async (event) => {
    try {
      const data = new Uint8Array(event.data as ArrayBuffer);

      if (!remoteConn) {
        const version = data[0];
        const uuid = parseUUID(data, 1);
        if (uuid !== USER_ID) {
          socket.close();
          return;
        }
        let offset = 17;
        const optLen = data[offset];
        offset += 1 + optLen;
        offset += 1; // cmd
        const port = (data[offset] << 8) | data[offset + 1];
        offset += 2;
        const addrType = data[offset];
        offset += 1;

        let addr = "";
        if (addrType === 1) {
          addr = data.slice(offset, offset + 4).join(".");
          offset += 4;
        } else if (addrType === 2) {
          const len = data[offset];
          offset += 1;
          addr = new TextDecoder().decode(data.slice(offset, offset + len));
          offset += len;
        } else if (addrType === 3) {
          const parts: string[] = [];
          for (let i = 0; i < 8; i++) {
            parts.push(((data[offset] << 8) | data[offset + 1]).toString(16));
            offset += 2;
          }
          addr = parts.join(":");
        }

        remoteConn = await Deno.connect({ hostname: addr, port });

        socket.send(new Uint8Array([version, 0]));

        const payload = data.slice(offset);
        if (payload.length > 0) {
          await remoteConn.write(payload);
        }

        (async () => {
          try {
            const buf = new Uint8Array(16384);
            while (remoteConn) {
              const n = await remoteConn.read(buf);
              if (n === null) break;
              socket.send(buf.slice(0, n));
            }
          } catch (_e) {}
          try { socket.close(); } catch (_e) {}
        })();
      } else {
        await remoteConn.write(data);
      }
    } catch (e) {
      console.error(e);
      try { socket.close(); } catch (_e) {}
    }
  };

  socket.onclose = () => {
    try { remoteConn?.close(); } catch (_e) {}
  };

  return response;
});
