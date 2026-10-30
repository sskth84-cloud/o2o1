const { put, list } = require('@vercel/blob');
const NAME = 'o2o-data.json';

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
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
      await put(
        NAME,
        JSON.stringify({ updatedAt, fileName: String(b.fileName || ''), sheets: b.sheets }),
        { access: 'public', addRandomSuffix: false, allowOverwrite: true, contentType: 'application/json', cacheControlMaxAge: 60 }
      );
      return res.status(200).json({ ok: true, updatedAt });
    }
    const { blobs } = await list({ prefix: NAME });
    const f = blobs.find((x) => x.pathname === NAME);
    if (!f) return res.status(404).json({ error: 'ยังไม่มีข้อมูลที่อัปโหลด' });
    const r = await fetch(f.url + '?t=' + Date.now());
    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(await r.text());
  } catch (e) {
    return res.status(500).json({ error: 'เซิร์ฟเวอร์ผิดพลาด: ' + e.message });
  }
};
