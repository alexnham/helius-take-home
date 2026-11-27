// Measures LaserStream bytes for USDC / USDT / both, to price streaming. Run from the repo root after `npm install` in samples-ts:
//   node --env-file=.env docs/research/stream-cost/measure.js 960
// Bytes are the uncompressed protobuf SubscribeUpdate sizes, the basis Helius bills on (2 credits per 0.1 MB).
const SDK = require("node:path").join(__dirname, "../../../samples-ts/node_modules/helius-laserstream");
const { LaserstreamClient: NapiClient, CommitmentLevel } = require(SDK + "/index");
const { initProtobuf, decodeSubscribeUpdate } = require(SDK + "/proto-decoder");

const USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const SUBS = { a_USDC: [USDC], b_USDT: [USDT], c_both: [USDC, USDT] };
const SECONDS = Number(process.argv[2] ?? 900);

// Top-level oneof field number of a SubscribeUpdate, read straight off the wire (4=transaction, 7=blockMeta).
function kind(buf) {
  let i = 0;
  const varint = () => { let v = 0, s = 0, b; do { b = buf[i++]; v += (b & 0x7f) * 2 ** s; s += 7; } while (b & 0x80); return v; };
  while (i < buf.length) {
    const tag = varint(), field = Math.floor(tag / 8), wt = tag & 7;
    if (field !== 1 && field !== 11) return field;
    if (wt === 2) { const len = varint(); i += len; } else if (wt === 0) varint(); else throw new Error("unexpected wire type " + wt);
  }
  return 0;
}

const stats = {};
const errors = [];
let checked = 0;

async function main() {
  await initProtobuf();
  const start = new Date();
  for (const [name, mints] of Object.entries(SUBS)) {
    const s = (stats[name] = { tx: 0, txBytes: 0, meta: 0, metaBytes: 0, other: 0, otherBytes: 0, firstSlot: null, lastSlot: null });
    const client = new NapiClient(process.env.LASERSTREAM_ENDPOINT, process.env.HELIUS_API_KEY, undefined, undefined, false, undefined);
    const request = {
      commitment: CommitmentLevel.FINALIZED,
      transactions: { transfers: { accountInclude: mints, accountExclude: [], accountRequired: [], matchMints: true, vote: false, failed: false } },
      blocksMeta: { everySlot: {} },
    };
    await client.subscribe(request, (err, bytes) => {
      if (err) { errors.push(`${name}: ${err}`); console.error(new Date().toISOString(), name, "error", String(err)); return; }
      const k = kind(bytes);
      if (checked < 300) { // self-check the wire parser against the SDK's protobufjs decode
        const d = decodeSubscribeUpdate(bytes);
        if ((k === 4) !== !!d.transaction || (k === 7) !== !!d.blockMeta) throw new Error("kind mismatch k=" + k + " keys=" + Object.keys(d).filter(x => d[x] !== undefined && x !== "filters").join(",") + " hex=" + Buffer.from(bytes).subarray(0, 24).toString("hex"));
        checked++;
      }
      if (k === 4) { s.tx++; s.txBytes += bytes.length; }
      else if (k === 7) {
        s.meta++; s.metaBytes += bytes.length;
        const slot = Number(decodeSubscribeUpdate(bytes).blockMeta.slot);
        s.firstSlot ??= slot; s.lastSlot = slot;
      } else { s.other++; s.otherBytes += bytes.length; }
    });
  }
  console.log("start", start.toISOString());
  const t0 = Date.now();
  const tick = setInterval(() => console.log(new Date().toISOString(), JSON.stringify(Object.fromEntries(Object.entries(stats).map(([n, s]) => [n, [s.tx, s.meta]])))), 60_000);
  setTimeout(() => {
    clearInterval(tick);
    const secs = (Date.now() - t0) / 1000;
    const out = { start: start.toISOString(), end: new Date().toISOString(), secs, checked, errors, subs: {} };
    for (const [n, s] of Object.entries(stats)) {
      const total = s.txBytes + s.metaBytes + s.otherBytes;
      const gbDay = (total / secs) * 86400 / 1e9;
      out.subs[n] = { ...s, totalBytes: total, txPerSec: s.tx / secs, avgKBPerTx: s.txBytes / s.tx / 1e3, avgBytesPerMeta: s.metaBytes / s.meta,
        MBperMin: total / secs * 60 / 1e6, GBperDay: gbDay, credits30d: (gbDay * 1e9 / 1e5) * 2 * 30,
        txOnlyGBperDay: s.txBytes / secs * 86400 / 1e9, metaGBperDay: s.metaBytes / secs * 86400 / 1e9 };
    }
    console.log("RESULT", JSON.stringify(out, null, 2));
    process.exit(0);
  }, SECONDS * 1000);
}
main().catch((e) => { console.error(e); process.exit(1); });
