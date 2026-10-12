const assert = require("node:assert/strict");
const { afterEach, beforeEach, test } = require("node:test");
const { Transport } = require("../.test-build/transport.js");
const { AuthError, ConnectionError } = require("../.test-build/errors.js");

class Socket {
  static OPEN = 1;
  static instances = [];
  readyState = Socket.OPEN;
  sent = [];

  constructor(url) {
    this.url = url;
    Socket.instances.push(this);
  }

  send(raw) {
    this.sent.push(JSON.parse(raw));
  }

  close() {
    this.readyState = 3;
    this.onclose?.();
  }

  receive(frame) {
    this.onmessage({ data: JSON.stringify(frame) });
  }
}

let originalSocket;
let transports;
beforeEach(() => {
  originalSocket = globalThis.WebSocket;
  globalThis.WebSocket = Socket;
  Socket.instances = [];
  transports = [];
});
afterEach(() => {
  for (const transport of transports) transport.close();
  globalThis.WebSocket = originalSocket;
});

function connecting() {
  const transport = new Transport({
    url: "ws://fixture/dwp",
    token: "test",
    reconnect: false,
  });
  transports.push(transport);
  const promise = transport.connect();
  // Attach the rejection observer before delivering synchronous fixture frames.
  const outcome = promise.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  const socket = Socket.instances.at(-1);
  socket.onopen();
  return { transport, socket, promise, outcome, authID: socket.sent[0].id };
}

function reply(id, fields = {}) {
  return {
    id: "reply",
    type: "response",
    correl_id: id,
    ts: "2026-10-09T00:00:00Z",
    ...fields,
  };
}

async function connected() {
  const fixture = connecting();
  fixture.socket.receive(
    reply(fixture.authID, { data: { session_id: "session", format: "json" } }),
  );
  assert.equal(await fixture.promise, "session");
  return fixture;
}

for (const value of [
  null,
  false,
  42,
  "text",
  [],
  {},
  { type: null },
  { type: "unknown" },
]) {
  test(`uncorrelated malformed JSON value is ignored: ${JSON.stringify(value)}`, async () => {
    const { socket, transport, promise, authID } = connecting();
    assert.doesNotThrow(() => socket.receive(value));
    assert.equal(transport.pending.size, 1);
    socket.receive(
      reply(authID, { data: { session_id: "valid", format: "json" } }),
    );
    assert.equal(await promise, "valid");
  });
}

test("invalid JSON syntax is ignored", async () => {
  const { socket, transport, promise, authID } = connecting();
  assert.doesNotThrow(() => socket.onmessage({ data: "{" }));
  assert.equal(transport.pending.size, 1);
  socket.receive(
    reply(authID, { data: { session_id: "valid", format: "json" } }),
  );
  assert.equal(await promise, "valid");
});

for (const data of [
  undefined,
  null,
  [],
  {},
  { session_id: "", format: "json" },
  { session_id: 123, format: "json" },
  { session_id: "session" },
  { session_id: "session", format: 1 },
  { session_id: "session", format: "msgpack" },
]) {
  test(`invalid authentication data rejects connect: ${JSON.stringify(data)}`, async () => {
    const { socket, transport, authID, outcome } = connecting();
    assert.doesNotThrow(() => socket.receive(reply(authID, { data })));
    const { error } = await outcome;
    assert.ok(error instanceof ConnectionError);
    assert.equal(transport.getSessionId(), "");
    assert.equal(transport.pending.size, 0);
  });
}

const malformed = [
  { type: undefined },
  { type: "unknown" },
  { type: null },
  { type: 1 },
  { id: null },
  { id: "" },
  { id: 42 },
  { ts: null },
  { correl_id: null },
  { correl_id: "" },
  { correl_id: 42 },
  { correl_id: {} },
  { correl_id: [] },
  { type: "error" },
  { type: "error", error: null },
  { type: "error", error: 42 },
  { type: "error", error: [] },
  { type: "error", error: {} },
  { type: "error", error: { code: "401", message: "denied" } },
  { type: "error", error: { code: 401.5, message: "denied" } },
  {
    type: "error",
    error: { code: 401, message: { toString: "not callable" } },
  },
  { type: "error", error: { code: 401, message: null } },
  { error: { code: 500, message: 42 } },
  { channel: {} },
];
for (const fields of malformed) {
  for (const operation of ["auth", "request"]) {
    test(`${operation} malformed correlated reply settles once: ${JSON.stringify(fields)}`, async () => {
      const fixture = operation === "auth" ? connecting() : await connected();
      const { socket, transport } = fixture;
      const promise =
        operation === "auth" ? fixture.promise : transport.request("stats");
      const id = socket.sent.at(-1).id;
      const outcome = promise.then(
        (value) => ({ value }),
        (error) => ({ error }),
      );
      // id fallback still identifies the operation when correl_id is malformed.
      const frame = reply(id, { id, ...fields });
      let settlements = 0;
      void outcome.then(() => settlements++);
      assert.doesNotThrow(() => socket.receive(frame));
      const { error } = await outcome;
      assert.ok(error instanceof ConnectionError);
      assert.equal(transport.pending.size, 0);
      socket.receive(frame);
      socket.receive(
        reply(id, { data: { session_id: "late", format: "json" } }),
      );
      await Promise.resolve();
      assert.equal(settlements, 1);
      assert.equal(
        transport.getSessionId(),
        operation === "auth" ? "" : "session",
      );
    });
  }
}

test("unidentifiable correlation IDs leave intended pending requests alone", async () => {
  const { socket, transport } = await connected();
  const pending = transport.request("stats");
  const id = socket.sent.at(-1).id;
  for (const correl_id of [null, "", 42, {}, []]) {
    assert.doesNotThrow(() => socket.receive(reply("unused", { correl_id })));
    assert.equal(transport.pending.size, 1);
  }
  socket.receive(reply(id, { data: "ok" }));
  assert.equal((await pending).data, "ok");
});

test("prototype names select no callback; real keys select only their resolver", async () => {
  const { socket, transport } = await connected();
  const first = transport.request("stats");
  const firstID = socket.sent.at(-1).id;
  const second = transport.request("job.list");
  const secondID = socket.sent.at(-1).id;
  let firstCalls = 0;
  let secondCalls = 0;
  void first.then(() => firstCalls++);
  void second.then(() => secondCalls++);
  for (const key of ["__proto__", "constructor", "toString"]) {
    assert.doesNotThrow(() => socket.receive(reply(key)));
  }
  await Promise.resolve();
  assert.equal(firstCalls, 0);
  assert.equal(secondCalls, 0);
  assert.equal(transport.pending.size, 2);
  socket.receive(reply(secondID, { data: "second" }));
  assert.equal((await second).data, "second");
  assert.equal(secondCalls, 1);
  assert.equal(firstCalls, 0);
  assert.equal(transport.pending.size, 1);
  socket.receive(reply(firstID, { data: "first" }));
  assert.equal((await first).data, "first");
  assert.equal(firstCalls, 1);
  assert.equal(transport.pending.size, 0);
});

test("valid auth error retains its public error type and message", async () => {
  const { socket, authID, outcome, transport } = connecting();
  socket.receive(
    reply(authID, { type: "error", error: { code: 401, message: "denied" } }),
  );
  const { error } = await outcome;
  assert.ok(error instanceof AuthError);
  assert.equal(error.message, "denied");
  assert.equal(error.code, 401);
  assert.equal(transport.pending.size, 0);
});

test("valid request error preserves numeric code and message", async () => {
  const { socket, transport } = await connected();
  const promise = transport.request("stats");
  const result = promise.catch((error) => error);
  socket.receive(
    reply(socket.sent.at(-1).id, {
      type: "error",
      error: { code: 403, message: "forbidden" },
    }),
  );
  const error = await result;
  assert.equal(error.message, "forbidden");
  assert.equal(error.code, 403);
  assert.equal(transport.pending.size, 0);
});

test("response id fallback resolves its request", async () => {
  const { socket, transport } = await connected();
  const promise = transport.request("stats");
  socket.receive(
    reply(undefined, { id: socket.sent.at(-1).id, data: { jobs: 3 } }),
  );
  assert.deepEqual((await promise).data, { jobs: 3 });
  assert.equal(transport.pending.size, 0);
});

test("application event handler exceptions remain observable", async () => {
  const { socket, transport } = await connected();
  const error = new Error("application handler");
  transport.onEvent("jobs", () => {
    throw error;
  });
  assert.throws(
    () => socket.receive(reply(undefined, { type: "event", channel: "jobs" })),
    (received) => received === error,
  );
});
