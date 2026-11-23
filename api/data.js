const Redis = require('ioredis');
const KEY = 'o2o-data';
const CHUNK = 250000; // ตัวอักษรต่อชิ้น (กันเกินขีดจำกัดขนาดคำสั่งของ Redis ฟรี)
const DATE = /^\d{4}-\d{2}-\d{2}$/;

async function setBig(r, key, val) {
  const parts = [];
  for (let i = 0; i < val.length; i += CHUNK) parts.push(val.slice(i, i + CHUNK));
  const old = parseInt(await r.get(key + ':n'), 10) || 0;
  for (let i = 0; i < parts.length; i++) await r.set(key + ':' + i, parts[i]);
  await r.set(key + ':n', String(parts.length));
  for (let i = parts.length; i < old; i++) await r.del(key + ':' + i);
  await r.del(key);
  return parts.length;
}

async function getBig(r, key) {
  const n = parseInt(await r.get(key + ':n'), 10) || 0;
  if (n > 0) {
    const keys = [];
    for (let i = 0; i < n; i++) keys.push(key + ':' + i);
    const got = await r.mget(keys);
    return got.every((x) => x != null) ? got.join('') : null;
  }
  return await r.get(key);
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  let url = process.env.REDIS_URL;
  if (!url) {
    return res.status(500).json({ error: 'ยังไม่ได้เชื่อมฐานข้อมูล Redis (ไม่พบตัวแปร REDIS_URL)' });
  }
  if (/upstash\.io/.test(url)) url = url.replace(/^redis:/, 'rediss:');
  const r = new Redis(url, { maxRetriesPerRequest: 2, connectTimeout: 8000 });
  const q = req.query || {};
  try {
    if (req.method === 'POST') {
      const b = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
      if (!process.env.UPLOAD_PASSWORD || b.password !== process.env.UPLOAD_PASSWORD) {
        return res.status(401).json({ error: 'รหัสผ่านไม่ถูกต้อง' });
      }
      const updatedAt = new Date().toISOString();
      if (q.k === 'daily' || q.k === 'ontime') {
        const P = q.k === 'ontime' ? 'o2o-x' : 'o2o-d';
        // ยอดขายประจำวัน (daily) / On-time OrderExport (ontime) แยกที่เก็บกัน: เก็บทีละวัน (days = { 'YYYY-MM-DD': { S, out?, ox?, fx? } })
        if (!b.days || typeof b.days !== 'object') return res.status(400).json({ error: 'ไม่พบข้อมูลรายวัน' });
        const saved = [];
        for (const d of Object.keys(b.days)) {
          if (!DATE.test(d)) continue;
          const rec = b.days[d] || {};
          const S = rec.S || {};
          await setBig(r, P + ':' + d, JSON.stringify({ updatedAt, fileName: String(b.fileName || ''), S, out: rec.out, ox: rec.ox, fx: rec.fx }));
          await r.set(P + ':m:' + d, JSON.stringify({ d, o: S.o || 0, cm: S.cm || 0, cx: S.cx || 0, sales: S.sales || 0, col: S.col || 0, updatedAt, fileName: String(b.fileName || ''), names: !!rec.out }));
          await r.sadd(P + ':idx', d);
          saved.push(d);
        }
        return res.status(200).json({ ok: true, updatedAt, days: saved });
      }
      if (!b.sheets || typeof b.sheets !== 'object') {
        return res.status(400).json({ error: 'ไม่พบข้อมูลชีต' });
      }
      const parts = await setBig(r, KEY, JSON.stringify({ updatedAt, fileName: String(b.fileName || ''), sheets: b.sheets }));
      return res.status(200).json({ ok: true, updatedAt, parts });
    }
    if (q.k === 'daily' || q.k === 'ontime') {
      const P = q.k === 'ontime' ? 'o2o-x' : 'o2o-d';
      if (q.d) {
        if (!DATE.test(String(q.d))) return res.status(400).json({ error: 'วันที่ไม่ถูกต้อง' });
        const v = await getBig(r, P + ':' + q.d);
        if (!v) return res.status(404).json({ error: 'ไม่พบข้อมูลวันที่นี้' });
        res.setHeader('Content-Type', 'application/json');
        return res.status(200).send(v);
      }
      const dates = (await r.smembers(P + ':idx')).filter((d) => DATE.test(d)).sort();
      const metas = [];
      for (const d of dates) {
        const m = await r.get(P + ':m:' + d);
        if (m) metas.push(JSON.parse(m));
      }
      return res.status(200).json({ days: metas });
    }
    const v = await getBig(r, KEY);
    if (!v) return res.status(404).json({ error: 'ยังไม่มีข้อมูลที่อัปโหลด' });
    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(v);
  } catch (e) {
    return res.status(500).json({ error: 'เซิร์ฟเวอร์ผิดพลาด: ' + e.message });
  } finally {
    r.disconnect();
  }
};
