const Redis = require('ioredis');
const KEY = 'o2o-data';

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  let url = process.env.REDIS_URL;
  if (!url) {
    return res.status(500).json({ error: 'ยังไม่ได้เชื่อมฐานข้อมูล Redis (ไม่พบตัวแปร REDIS_URL)' });
  }
  if (/upstash\.io/.test(url)) url = url.replace(/^redis:/, 'rediss:');
  const r = new Redis(url, { maxRetriesPerRequest: 2, connectTimeout: 8000 });
  try {
    if (req.method === 'POST') {
      const b = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
      if (!process.env.UPLOAD_PASSWORD || b.password !== process.env.UPLOAD_PASSWORD) {
        return res.status(401).json({ error: 'รหัสผ่านไม่ถูกต้อง' });
      }
      if (!b.sheets || typeof b.sheets !== 'object') {
        return res.status(400).json({ error: 'ไม่พบข้อมูลชีต' });
      }
      const updatedAt = new Date().toISOString();
      await r.set(KEY, JSON.stringify({ updatedAt, fileName: String(b.fileName || ''), sheets: b.sheets }));
      return res.status(200).json({ ok: true, updatedAt });
    }
    const v = await r.get(KEY);
    if (!v) return res.status(404).json({ error: 'ยังไม่มีข้อมูลที่อัปโหลด' });
    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(v);
  } catch (e) {
    return res.status(500).json({ error: 'เซิร์ฟเวอร์ผิดพลาด: ' + e.message });
  } finally {
    r.disconnect();
  }
};
