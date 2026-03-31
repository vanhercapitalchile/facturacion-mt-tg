'use strict';
const express  = require('express');
const path     = require('path');
const crypto   = require('crypto');
const jwt      = require('jsonwebtoken');
const Database = require('better-sqlite3');
const fs       = require('fs');
const multer   = require('multer');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Config ───────────────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || 'facturacion-mt-tg-dev-secret-change-me';
const DATA_DIR   = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(path.join(DATA_DIR, 'app.db'));
db.pragma('journal_mode = WAL');

// Upload config
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// ── Schema ───────────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS app_data (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS login_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    display_name TEXT,
    empresa TEXT,
    timestamp_cl TEXT NOT NULL,
    read_by_admin INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    empresa_id TEXT NOT NULL,
    id_transferencia TEXT,
    fecha TEXT,
    monto REAL,
    glosa TEXT,
    rut TEXT,
    rut_normalizado TEXT,
    nombre_origen TEXT,
    banco_origen TEXT,
    banco_cartola TEXT,
    cuenta_origen TEXT,
    id_compuesto TEXT UNIQUE,
    estado TEXT DEFAULT 'pendiente',
    tipo_dte INTEGER,
    razon_social TEXT,
    giro TEXT,
    direccion TEXT,
    comuna TEXT,
    ciudad TEXT,
    email_receptor TEXT,
    nombre_item TEXT,
    descripcion_item TEXT,
    precio REAL,
    monto_exento REAL,
    monto_total REAL,
    folio_dte TEXT,
    fecha_emision TEXT,
    fecha_carga TEXT,
    fecha_facturacion TEXT,
    lote_id TEXT,
    created_at TEXT,
    updated_at TEXT
  );
  CREATE TABLE IF NOT EXISTS clientes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    empresa_id TEXT NOT NULL,
    tipo TEXT DEFAULT 'empresa',
    rut TEXT,
    rut_normalizado TEXT,
    razon_social TEXT,
    giro TEXT,
    direccion TEXT,
    comuna TEXT,
    ciudad TEXT,
    nombre TEXT,
    email TEXT,
    telefono TEXT,
    representante_legal TEXT,
    rut_representante TEXT,
    created_at TEXT,
    updated_at TEXT
  );
  CREATE TABLE IF NOT EXISTS lotes_facturacion (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote_id TEXT UNIQUE,
    empresa_id TEXT NOT NULL,
    cantidad INTEGER,
    monto_total REAL,
    estado TEXT DEFAULT 'pendiente',
    metodo TEXT DEFAULT 'manual',
    response_api TEXT,
    created_at TEXT,
    updated_at TEXT
  );
`);

// Indexes
try { db.exec('CREATE INDEX IF NOT EXISTS idx_mov_empresa ON movimientos(empresa_id)'); } catch(e){}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_mov_id_compuesto ON movimientos(id_compuesto)'); } catch(e){}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_mov_estado ON movimientos(estado)'); } catch(e){}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_clientes_rut ON clientes(rut_normalizado, empresa_id)'); } catch(e){}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_mov_lote ON movimientos(lote_id)'); } catch(e){}

// ── Seed ─────────────────────────────────────────────────────────────────────
(function seedIfEmpty() {
  if (db.prepare('SELECT value FROM app_data WHERE key = ?').get('users')) return;
  const seedPath = path.join(__dirname, 'seed-data.json');
  if (!fs.existsSync(seedPath)) return;
  const seed = JSON.parse(fs.readFileSync(seedPath, 'utf8'));
  const stmt = db.prepare('INSERT OR REPLACE INTO app_data (key, value) VALUES (?, ?)');
  db.transaction(() => {
    stmt.run('users', JSON.stringify(seed.users));
    stmt.run('empresas', JSON.stringify(seed.empresas));
    stmt.run('config', JSON.stringify(seed.config));
  })();
  console.log('[SEED] Database initialised from seed-data.json');
})();

// ── Helpers ──────────────────────────────────────────────────────────────────
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

function sha256(str) { return crypto.createHash('sha256').update(str).digest('hex'); }

function nowCL() {
  return new Date().toLocaleString('es-CL', {
    timeZone: 'America/Santiago',
    day: '2-digit', month: '2-digit', year: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit'
  });
}

function todayCL() {
  return new Date().toLocaleDateString('es-CL', {
    timeZone: 'America/Santiago', year: 'numeric', month: '2-digit', day: '2-digit'
  }).split('-').reverse().join('-');
}

function getAppData(key) {
  const row = db.prepare('SELECT value FROM app_data WHERE key = ?').get(key);
  return row ? JSON.parse(row.value) : {};
}

function setAppData(key, value) {
  db.prepare('INSERT OR REPLACE INTO app_data (key, value) VALUES (?, ?)').run(key, JSON.stringify(value));
}

function requireAuth(req, res, next) {
  const h = req.headers.authorization;
  if (!h?.startsWith('Bearer ')) return res.status(401).json({ error: 'No token' });
  try {
    req.user = jwt.verify(h.slice(7), JWT_SECRET);
    next();
  } catch { res.status(401).json({ error: 'Token inválido o expirado' }); }
}

function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo administradores' });
  next();
}

function filterByEmpresa(req) {
  if (req.user.role === 'admin') return req.query.empresa_id || null;
  return req.user.empresa;
}

function normalizeRut(rut) {
  if (!rut) return '';
  return String(rut).replace(/[.\-\s]/g, '').toUpperCase();
}

function formatRut(rut) {
  const clean = normalizeRut(rut);
  if (clean.length < 2) return rut;
  const body = clean.slice(0, -1);
  const dv = clean.slice(-1);
  const formatted = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return formatted + '-' + dv;
}

function generateLoteId(empresaId) {
  const ts = Date.now().toString(36);
  const rnd = Math.random().toString(36).slice(2, 6);
  return `${empresaId}-${ts}-${rnd}`;
}

// ── Auth Routes ──────────────────────────────────────────────────────────────
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Faltan credenciales' });

  const users = getAppData('users');
  const user = users[username.toLowerCase()];
  if (!user) return res.status(401).json({ error: 'Credenciales inválidas' });

  const hash = sha256(password);
  if (hash !== user.passHash) return res.status(401).json({ error: 'Credenciales inválidas' });

  const payload = { username: username.toLowerCase(), role: user.role, empresa: user.empresa };
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '4h' });

  const empresas = getAppData('empresas');
  const config = getAppData('config');

  res.json({
    token,
    user: { ...payload, nombre: user.nombre, initials: user.initials },
    empresas,
    config
  });
});

// ── Notification routes ──────────────────────────────────────────────────────
app.post('/api/notifications/login', requireAuth, (req, res) => {
  const { displayName } = req.body;
  db.prepare('INSERT INTO login_logs (username, display_name, empresa, timestamp_cl) VALUES (?,?,?,?)')
    .run(req.user.username, displayName || req.user.nombre, req.user.empresa, nowCL());
  res.json({ ok: true });
});

app.get('/api/notifications', requireAuth, requireAdmin, (req, res) => {
  const logs = db.prepare('SELECT * FROM login_logs ORDER BY id DESC LIMIT 100').all();
  const unread = db.prepare('SELECT COUNT(*) AS cnt FROM login_logs WHERE read_by_admin = 0').get().cnt;
  res.json({ logs, unread });
});

app.put('/api/notifications/read', requireAuth, requireAdmin, (req, res) => {
  db.prepare('UPDATE login_logs SET read_by_admin = 1 WHERE read_by_admin = 0').run();
  res.json({ ok: true });
});

// ── Data routes (app_data CRUD) ──────────────────────────────────────────────
app.get('/api/data', requireAuth, (req, res) => {
  const empresas = getAppData('empresas');
  const config = getAppData('config');
  if (req.user.role === 'admin') {
    const users = getAppData('users');
    return res.json({ users, empresas, config });
  }
  const myEmpresa = empresas[req.user.empresa] || null;
  res.json({ empresa: myEmpresa, empresaId: req.user.empresa, config });
});

app.put('/api/data', requireAuth, requireAdmin, (req, res) => {
  const { users, empresas, config } = req.body;
  db.transaction(() => {
    if (users) setAppData('users', users);
    if (empresas) setAppData('empresas', empresas);
    if (config) setAppData('config', config);
  })();
  res.json({ ok: true });
});

// ── Users management ─────────────────────────────────────────────────────────
app.get('/api/users', requireAuth, requireAdmin, (req, res) => {
  res.json(getAppData('users'));
});

app.put('/api/users', requireAuth, requireAdmin, (req, res) => {
  setAppData('users', req.body);
  res.json({ ok: true });
});

// ── Clientes (base de datos tributaria) ──────────────────────────────────────
app.get('/api/clientes', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  let rows;
  if (empresaId) {
    rows = db.prepare('SELECT * FROM clientes WHERE empresa_id = ? ORDER BY razon_social').all(empresaId);
  } else {
    rows = db.prepare('SELECT * FROM clientes ORDER BY empresa_id, razon_social').all();
  }
  res.json(rows);
});

app.post('/api/clientes', requireAuth, (req, res) => {
  const c = req.body;
  const empresaId = req.user.role === 'admin' ? c.empresa_id : req.user.empresa;
  const now = nowCL();
  const rutNorm = normalizeRut(c.rut);
  const result = db.prepare(`
    INSERT INTO clientes (empresa_id, tipo, rut, rut_normalizado, razon_social, giro, direccion, comuna, ciudad, nombre, email, telefono, representante_legal, rut_representante, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `).run(empresaId, c.tipo || 'empresa', c.rut, rutNorm, c.razon_social, c.giro, c.direccion, c.comuna, c.ciudad, c.nombre, c.email, c.telefono, c.representante_legal, c.rut_representante, now, now);
  res.json({ ok: true, id: result.lastInsertRowid });
});

app.put('/api/clientes/:id', requireAuth, (req, res) => {
  const c = req.body;
  const now = nowCL();
  db.prepare(`
    UPDATE clientes SET tipo=?, rut=?, rut_normalizado=?, razon_social=?, giro=?, direccion=?, comuna=?, ciudad=?, nombre=?, email=?, telefono=?, representante_legal=?, rut_representante=?, updated_at=?
    WHERE id=?
  `).run(c.tipo, c.rut, normalizeRut(c.rut), c.razon_social, c.giro, c.direccion, c.comuna, c.ciudad, c.nombre, c.email, c.telefono, c.representante_legal, c.rut_representante, now, req.params.id);
  res.json({ ok: true });
});

app.delete('/api/clientes/:id', requireAuth, requireAdmin, (req, res) => {
  db.prepare('DELETE FROM clientes WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

// Bulk import clientes from uploaded Excel data
app.post('/api/clientes/import', requireAuth, (req, res) => {
  const { empresa_id, clientes: clientesList } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  const now = nowCL();
  let imported = 0, skipped = 0;

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO clientes (empresa_id, tipo, rut, rut_normalizado, razon_social, giro, direccion, comuna, ciudad, nombre, email, telefono, representante_legal, rut_representante, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const checkStmt = db.prepare('SELECT id FROM clientes WHERE rut_normalizado = ? AND empresa_id = ?');

  db.transaction(() => {
    for (const c of clientesList) {
      const rutNorm = normalizeRut(c.rut);
      if (!rutNorm) { skipped++; continue; }
      const exists = checkStmt.get(rutNorm, empresaId);
      if (exists) { skipped++; continue; }
      insertStmt.run(empresaId, c.tipo || 'empresa', c.rut, rutNorm, c.razon_social, c.giro, c.direccion, c.comuna, c.ciudad, c.nombre, c.email, c.telefono, c.representante_legal, c.rut_representante, now, now);
      imported++;
    }
  })();

  res.json({ ok: true, imported, skipped });
});

// Search client by RUT
app.get('/api/clientes/buscar/:rut', requireAuth, (req, res) => {
  const rutNorm = normalizeRut(req.params.rut);
  const empresaId = filterByEmpresa(req);
  let row;
  if (empresaId) {
    row = db.prepare('SELECT * FROM clientes WHERE rut_normalizado = ? AND empresa_id = ?').get(rutNorm, empresaId);
  } else {
    row = db.prepare('SELECT * FROM clientes WHERE rut_normalizado = ?').get(rutNorm);
  }
  res.json(row || null);
});

// ── Movimientos (transferencias de cartola) ──────────────────────────────────
app.get('/api/movimientos', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  const { estado, fecha_desde, fecha_hasta, lote_id, limit: lim, offset: off } = req.query;
  let sql = 'SELECT * FROM movimientos WHERE 1=1';
  const params = [];

  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  if (estado) { sql += ' AND estado = ?'; params.push(estado); }
  if (fecha_desde) { sql += ' AND fecha >= ?'; params.push(fecha_desde); }
  if (fecha_hasta) { sql += ' AND fecha <= ?'; params.push(fecha_hasta); }
  if (lote_id) { sql += ' AND lote_id = ?'; params.push(lote_id); }

  sql += ' ORDER BY id DESC';
  if (lim) { sql += ' LIMIT ?'; params.push(parseInt(lim)); }
  if (off) { sql += ' OFFSET ?'; params.push(parseInt(off)); }

  const rows = db.prepare(sql).all(...params);
  res.json(rows);
});

app.get('/api/movimientos/stats', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  let where = '1=1';
  const params = [];
  if (empresaId) { where += ' AND empresa_id = ?'; params.push(empresaId); }

  const total = db.prepare(`SELECT COUNT(*) as cnt FROM movimientos WHERE ${where}`).get(...params).cnt;
  const pendientes = db.prepare(`SELECT COUNT(*) as cnt FROM movimientos WHERE ${where} AND estado = 'pendiente'`).get(...params).cnt;
  const listos = db.prepare(`SELECT COUNT(*) as cnt FROM movimientos WHERE ${where} AND estado = 'listo'`).get(...params).cnt;
  const facturados = db.prepare(`SELECT COUNT(*) as cnt FROM movimientos WHERE ${where} AND estado = 'facturado'`).get(...params).cnt;
  const yaEmitidos = db.prepare(`SELECT COUNT(*) as cnt FROM movimientos WHERE ${where} AND estado = 'ya_emitido'`).get(...params).cnt;
  const montoTotal = db.prepare(`SELECT COALESCE(SUM(monto),0) as s FROM movimientos WHERE ${where}`).get(...params).s;
  const montoFacturado = db.prepare(`SELECT COALESCE(SUM(monto),0) as s FROM movimientos WHERE ${where} AND estado = 'facturado'`).get(...params).s;
  const montoPendiente = db.prepare(`SELECT COALESCE(SUM(monto),0) as s FROM movimientos WHERE ${where} AND estado IN ('pendiente','listo')`).get(...params).s;

  res.json({ total, pendientes, listos, facturados, yaEmitidos, montoTotal, montoFacturado, montoPendiente });
});

// Upload cartola and process
app.post('/api/movimientos/cargar-cartola', requireAuth, upload.single('cartola'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
    const empresaId = req.user.role === 'admin' ? (req.body.empresa_id || '') : req.user.empresa;
    if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });
    const bancoCartola = (req.body.banco || '').toUpperCase();
    if (!bancoCartola) return res.status(400).json({ error: 'Banco no especificado' });

    // Return the raw file to be parsed on the frontend (SheetJS)
    // The frontend parses the Excel, sends structured data to /api/movimientos/procesar
    res.json({
      ok: true,
      message: 'Archivo recibido. El parsing se realiza en el frontend.',
      filename: req.file.originalname,
      size: req.file.size,
      buffer: req.file.buffer.toString('base64')
    });
  } catch (err) {
    console.error('[CARTOLA ERROR]', err);
    res.status(500).json({ error: 'Error al procesar cartola: ' + err.message });
  }
});

// Process parsed movements from frontend
app.post('/api/movimientos/procesar', requireAuth, (req, res) => {
  const { empresa_id, banco_cartola, movimientos: movs } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });
  const now = nowCL();
  const config = getAppData('config');

  let nuevos = 0, duplicados = 0, errores = 0;
  const results = [];

  const checkDup = db.prepare('SELECT id, estado FROM movimientos WHERE id_compuesto = ?');
  const insertMov = db.prepare(`
    INSERT INTO movimientos (empresa_id, id_transferencia, fecha, monto, glosa, rut, rut_normalizado, nombre_origen, banco_origen, banco_cartola, cuenta_origen, id_compuesto, estado, tipo_dte, razon_social, giro, direccion, comuna, ciudad, email_receptor, nombre_item, descripcion_item, precio, monto_exento, monto_total, fecha_carga, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  // Load clientes for matching
  const clientesEmpresa = db.prepare("SELECT * FROM clientes WHERE empresa_id = ? AND tipo = 'empresa'").all(empresaId);
  const clientesPersona = db.prepare("SELECT * FROM clientes WHERE empresa_id = ? AND tipo = 'persona'").all(empresaId);
  const clienteMap = new Map();
  for (const c of [...clientesEmpresa, ...clientesPersona]) {
    if (c.rut_normalizado) clienteMap.set(c.rut_normalizado, c);
  }

  db.transaction(() => {
    for (const mov of movs) {
      try {
        const idTransf = String(mov.id_transferencia || '').trim();
        const idCompuesto = `${idTransf}_${banco_cartola}`;

        // Check duplicate
        const existing = checkDup.get(idCompuesto);
        if (existing) {
          duplicados++;
          results.push({ id_compuesto: idCompuesto, status: 'duplicado', existing_estado: existing.estado });
          continue;
        }

        const rutNorm = normalizeRut(mov.rut || '');
        let estado = 'pendiente';
        let tipoDte = null;
        let razonSocial = '', giro = '', direccion = '', comuna = '', ciudad = '', emailReceptor = '';

        // Determine DTE type by RUT: RUT >= 76M → Factura Exenta (34), RUT < 76M → Boleta Exenta (41)
        if (rutNorm) {
          const rutNum = parseInt(rutNorm);
          tipoDte = rutNum >= 76000000 ? 34 : 41;

          const cliente = clienteMap.get(rutNorm);
          if (cliente) {
            // Client found in DB: use full client data, mark as ready
            estado = 'listo';
            razonSocial = cliente.razon_social || '';
            giro = cliente.giro || '';
            direccion = cliente.direccion || '';
            comuna = cliente.comuna || '';
            ciudad = cliente.ciudad || '';
            emailReceptor = cliente.email || '';
          } else if (tipoDte === 41) {
            // Boleta sin cliente en BD: usar nombre de la cartola como identificador
            razonSocial = (mov.nombre_origen || '').substring(0, 100) || 'SIN NOMBRE';
            estado = 'listo';
          }
          // Factura (34) sin cliente en BD: queda pendiente para revisión manual
        }

        const monto = parseFloat(mov.monto) || 0;
        const nombreItem = config.nombre_item_default || 'Venta paquete activo digital';
        const descripcionItem = `${config.descripcion_item_default || 'Venta paquete activo digital'} Banco ${banco_cartola}`;

        insertMov.run(
          empresaId, idTransf, mov.fecha || '', monto, mov.glosa || '',
          mov.rut ? formatRut(mov.rut) : '', rutNorm,
          mov.nombre_origen || '', mov.banco_origen || '', banco_cartola, mov.cuenta_origen || '',
          idCompuesto, estado, tipoDte,
          razonSocial, giro ? giro.substring(0, 80) : '', direccion ? direccion.substring(0, 100) : '',
          comuna, ciudad, emailReceptor,
          nombreItem, descripcionItem,
          monto, tipoDte ? monto : 0, monto,  // ambos tipo 34 y 41 son exentos
          now, now, now
        );
        nuevos++;
        results.push({ id_compuesto: idCompuesto, status: estado, rut: rutNorm });
      } catch (err) {
        errores++;
        results.push({ id_transferencia: mov.id_transferencia, status: 'error', error: err.message });
      }
    }
  })();

  res.json({ ok: true, nuevos, duplicados, errores, total: movs.length, results });
});

// Update single movimiento (edit client data, estado, etc.)
app.put('/api/movimientos/:id', requireAuth, (req, res) => {
  const m = req.body;
  const now = nowCL();
  db.prepare(`
    UPDATE movimientos SET rut=?, rut_normalizado=?, razon_social=?, giro=?, direccion=?, comuna=?, ciudad=?, email_receptor=?, tipo_dte=?, estado=?, nombre_item=?, descripcion_item=?, precio=?, monto_exento=?, monto_total=?, updated_at=?
    WHERE id=?
  `).run(m.rut, normalizeRut(m.rut), m.razon_social, m.giro, m.direccion, m.comuna, m.ciudad, m.email_receptor, m.tipo_dte, m.estado, m.nombre_item, m.descripcion_item, m.precio, m.monto_exento, m.monto_total, now, req.params.id);
  res.json({ ok: true });
});

// Bulk update estado
app.put('/api/movimientos/bulk-estado', requireAuth, (req, res) => {
  const { ids, estado } = req.body;
  const now = nowCL();
  const stmt = db.prepare('UPDATE movimientos SET estado = ?, updated_at = ? WHERE id = ?');
  db.transaction(() => {
    for (const id of ids) stmt.run(estado, now, id);
  })();
  res.json({ ok: true, updated: ids.length });
});

// ── Facturación (crear lote + enviar a SimpleFactura) ────────────────────────
app.post('/api/facturacion/crear-lote', requireAuth, (req, res) => {
  const { empresa_id, movimiento_ids } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  const now = nowCL();
  const loteId = generateLoteId(empresaId);

  // Get movimientos
  const placeholders = movimiento_ids.map(() => '?').join(',');
  const movs = db.prepare(`SELECT * FROM movimientos WHERE id IN (${placeholders}) AND estado = 'listo'`).all(...movimiento_ids);

  if (movs.length === 0) return res.status(400).json({ error: 'No hay movimientos listos para facturar' });

  const montoTotal = movs.reduce((s, m) => s + (m.monto_total || 0), 0);

  db.transaction(() => {
    // Create lote
    db.prepare('INSERT INTO lotes_facturacion (lote_id, empresa_id, cantidad, monto_total, estado, created_at, updated_at) VALUES (?,?,?,?,?,?,?)')
      .run(loteId, empresaId, movs.length, montoTotal, 'pendiente', now, now);
    // Update movimientos
    const updStmt = db.prepare("UPDATE movimientos SET lote_id = ?, estado = 'en_lote', updated_at = ? WHERE id = ?");
    for (const m of movs) updStmt.run(loteId, now, m.id);
  })();

  res.json({ ok: true, lote_id: loteId, cantidad: movs.length, monto_total: montoTotal });
});

// Get lotes
app.get('/api/facturacion/lotes', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  let sql = 'SELECT * FROM lotes_facturacion';
  const params = [];
  if (empresaId) { sql += ' WHERE empresa_id = ?'; params.push(empresaId); }
  sql += ' ORDER BY id DESC';
  res.json(db.prepare(sql).all(...params));
});

// Emit via SimpleFactura API
app.post('/api/facturacion/emitir/:lote_id', requireAuth, async (req, res) => {
  const loteId = req.params.lote_id;
  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });

  const empresas = getAppData('empresas');
  const empresa = empresas[lote.empresa_id];
  if (!empresa?.simplefactura?.username || !empresa?.simplefactura?.password) {
    return res.status(400).json({ error: 'Credenciales de SimpleFactura no configuradas para esta empresa' });
  }

  const movs = db.prepare("SELECT * FROM movimientos WHERE lote_id = ? AND estado = 'en_lote'").all(loteId);
  if (movs.length === 0) return res.status(400).json({ error: 'No hay movimientos en este lote' });

  const now = nowCL();
  const sfConfig = empresa.simplefactura;
  const baseUrl = getAppData('config').simplefactura_base_url || 'https://api.simplefactura.cl';

  // Build DTEs for SimpleFactura
  const dtes = movs.map(m => ({
    Encabezado: {
      IdDoc: { TipoDTE: m.tipo_dte, FchEmis: todayCL() },
      Emisor: { RUTEmisor: sfConfig.rut_emisor },
      Receptor: {
        RUTRecep: normalizeRut(m.rut),
        RznSocRecep: m.razon_social,
        GiroRecep: (m.giro || '').substring(0, 80),
        DirRecep: (m.direccion || '').substring(0, 100),
        CmnaRecep: m.comuna || '',
        CiudadRecep: m.ciudad || '',
        ContactoRecep: 'NO INFORMADO',
        CorreoRecep: m.email_receptor || empresa.email_facturacion
      }
    },
    Detalle: [{
      NmbItem: m.nombre_item || 'Venta paquete activo digital',
      DscItem: m.descripcion_item || '',
      QtyItem: 1,
      PrcItem: m.monto_total,
      MontoItem: m.monto_total,
      IndExe: m.tipo_dte === 34 ? 1 : 0
    }],
    _movimiento_id: m.id
  }));

  try {
    // Authenticate with SimpleFactura
    const authHeader = 'Basic ' + Buffer.from(`${sfConfig.username}:${sfConfig.password}`).toString('base64');

    const results = [];
    for (const dte of dtes) {
      const movId = dte._movimiento_id;
      delete dte._movimiento_id;
      try {
        const response = await fetch(`${baseUrl}/invoices`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': authHeader },
          body: JSON.stringify(dte)
        });
        const data = await response.json();
        if (response.ok) {
          db.prepare("UPDATE movimientos SET estado = 'facturado', folio_dte = ?, fecha_facturacion = ?, updated_at = ? WHERE id = ?")
            .run(data.folio || data.Folio || '', now, now, movId);
          results.push({ id: movId, status: 'ok', folio: data.folio || data.Folio });
        } else {
          results.push({ id: movId, status: 'error', error: data.message || JSON.stringify(data) });
        }
      } catch (err) {
        results.push({ id: movId, status: 'error', error: err.message });
      }
    }

    const exitosos = results.filter(r => r.status === 'ok').length;
    const fallidos = results.filter(r => r.status === 'error').length;
    const loteEstado = fallidos === 0 ? 'emitido' : (exitosos > 0 ? 'parcial' : 'error');

    db.prepare('UPDATE lotes_facturacion SET estado = ?, response_api = ?, updated_at = ? WHERE lote_id = ?')
      .run(loteEstado, JSON.stringify(results), now, loteId);

    res.json({ ok: true, exitosos, fallidos, results });
  } catch (err) {
    console.error('[SIMPLEFACTURA ERROR]', err);
    res.status(500).json({ error: 'Error al conectar con SimpleFactura: ' + err.message });
  }
});

// Mark lote as manually exported
app.post('/api/facturacion/marcar-exportado/:lote_id', requireAuth, (req, res) => {
  const now = nowCL();
  db.prepare("UPDATE lotes_facturacion SET estado = 'exportado_manual', metodo = 'csv_manual', updated_at = ? WHERE lote_id = ?")
    .run(now, req.params.lote_id);
  // Also update movimientos
  db.prepare("UPDATE movimientos SET estado = 'facturado', fecha_facturacion = ?, updated_at = ? WHERE lote_id = ?")
    .run(now, now, req.params.lote_id);
  res.json({ ok: true });
});

// ── Export CSV for SimpleFactura manual upload ───────────────────────────────
app.get('/api/facturacion/exportar-csv/:lote_id', requireAuth, (req, res) => {
  const loteId = req.params.lote_id;
  const movs = db.prepare('SELECT * FROM movimientos WHERE lote_id = ?').all(loteId);
  if (movs.length === 0) return res.status(404).json({ error: 'Lote sin movimientos' });

  const empresas = getAppData('empresas');
  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  const empresa = empresas[lote?.empresa_id] || {};

  // Build CSV in SimpleFactura format
  const headers = [
    'Id','TipoDte','FmaPago','FechaEmision','Vencimiento','RutRecep','GiroRecep','Contacto','CorreoRecep',
    'DirRecep','CmnaRecep','CiudadRecep','RazonSocialRecep','DirDest','CmnaDest','CiudadDest',
    'ReferenciaTpoDocRef','ReferenciaFolioRef','ReferenciaFchRef','ReferenciaRazonRef','ReferenciaCodigo',
    'CodigoProducto','NombreProducto','DescripcionProducto','CantidadProducto','PrecioProducto',
    'UnidadMedidaProducto','DescuentoProducto','RecargoProducto','RebajaAvaluo','IndicadorExento',
    'TotalProducto','GlosaDR','TpoMov','TpoValor','ValorDR','ValorOtrMnda','IndExeDR','Correo',
    'ID Transferencia','Cartola','Id Compuesto'
  ];

  const today = todayCL().split('-').reverse().join('-'); // DD-MM-YYYY
  const rows = movs.map((m, i) => {
    const fechaEmision = m.fecha_emision || today;
    return [
      i + 1, m.tipo_dte || 34, 1, fechaEmision, fechaEmision,
      normalizeRut(m.rut), (m.giro || '').substring(0, 80), 'NO INFORMADO',
      m.email_receptor || empresa.email_facturacion || '',
      (m.direccion || '').substring(0, 100), m.comuna || '', m.ciudad || '',
      m.razon_social || '', '', '', '',
      '', '', '', '', '',
      '', m.nombre_item || 'Venta paquete activo digital',
      m.descripcion_item || `Venta paquete activo digital Banco ${m.banco_cartola}`,
      1, m.monto_total || 0, 'UNID', 0, 0, 0,
      m.tipo_dte === 34 ? 1 : 0, m.monto_total || 0,
      '', '', '', '', '', '',
      m.email_receptor || '',
      m.id_transferencia || '', m.banco_cartola || '', m.id_compuesto || ''
    ];
  });

  let csv = headers.join(';') + '\n';
  for (const row of rows) csv += row.join(';') + '\n';

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Facturacion_${lote?.empresa_id}_${loteId}.csv"`);
  res.send('\uFEFF' + csv); // BOM for Excel
});

// Export movimientos as CSV (without lote - for manual selection)
app.post('/api/facturacion/exportar-seleccion', requireAuth, (req, res) => {
  const { movimiento_ids, empresa_id } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  const placeholders = movimiento_ids.map(() => '?').join(',');
  const movs = db.prepare(`SELECT * FROM movimientos WHERE id IN (${placeholders})`).all(...movimiento_ids);

  const empresas = getAppData('empresas');
  const empresa = empresas[empresaId] || {};

  const headers = [
    'Id','TipoDte','FmaPago','FechaEmision','Vencimiento','RutRecep','GiroRecep','Contacto','CorreoRecep',
    'DirRecep','CmnaRecep','CiudadRecep','RazonSocialRecep','DirDest','CmnaDest','CiudadDest',
    'ReferenciaTpoDocRef','ReferenciaFolioRef','ReferenciaFchRef','ReferenciaRazonRef','ReferenciaCodigo',
    'CodigoProducto','NombreProducto','DescripcionProducto','CantidadProducto','PrecioProducto',
    'UnidadMedidaProducto','DescuentoProducto','RecargoProducto','RebajaAvaluo','IndicadorExento',
    'TotalProducto','GlosaDR','TpoMov','TpoValor','ValorDR','ValorOtrMnda','IndExeDR','Correo',
    'ID Transferencia','Cartola','Id Compuesto'
  ];

  const today = todayCL().split('-').reverse().join('-');
  const rows = movs.map((m, i) => {
    const fechaEmision = m.fecha_emision || today;
    return [
      i + 1, m.tipo_dte || 34, 1, fechaEmision, fechaEmision,
      normalizeRut(m.rut), (m.giro || '').substring(0, 80), 'NO INFORMADO',
      m.email_receptor || empresa.email_facturacion || '',
      (m.direccion || '').substring(0, 100), m.comuna || '', m.ciudad || '',
      m.razon_social || '', '', '', '',
      '', '', '', '', '',
      '', m.nombre_item || 'Venta paquete activo digital',
      m.descripcion_item || `Venta paquete activo digital Banco ${m.banco_cartola}`,
      1, m.monto_total || 0, 'UNID', 0, 0, 0,
      m.tipo_dte === 34 ? 1 : 0, m.monto_total || 0,
      '', '', '', '', '', '',
      m.email_receptor || '',
      m.id_transferencia || '', m.banco_cartola || '', m.id_compuesto || ''
    ];
  });

  let csv = headers.join(';') + '\n';
  for (const row of rows) csv += row.join(';') + '\n';

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Facturacion_${empresaId}_seleccion.csv"`);
  res.send('\uFEFF' + csv);
});

// ── Dashboard stats ──────────────────────────────────────────────────────────
app.get('/api/dashboard', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  const stats = {};

  const empresaIds = empresaId ? [empresaId] : ['tg-inversiones', 'mt-inversiones'];

  for (const eid of empresaIds) {
    const total = db.prepare('SELECT COUNT(*) as cnt FROM movimientos WHERE empresa_id = ?').get(eid).cnt;
    const pendientes = db.prepare("SELECT COUNT(*) as cnt FROM movimientos WHERE empresa_id = ? AND estado = 'pendiente'").get(eid).cnt;
    const listos = db.prepare("SELECT COUNT(*) as cnt FROM movimientos WHERE empresa_id = ? AND estado = 'listo'").get(eid).cnt;
    const facturados = db.prepare("SELECT COUNT(*) as cnt FROM movimientos WHERE empresa_id = ? AND estado = 'facturado'").get(eid).cnt;
    const montoTotal = db.prepare('SELECT COALESCE(SUM(monto),0) as s FROM movimientos WHERE empresa_id = ?').get(eid).s;
    const montoFacturado = db.prepare("SELECT COALESCE(SUM(monto),0) as s FROM movimientos WHERE empresa_id = ? AND estado = 'facturado'").get(eid).s;
    const totalClientes = db.prepare('SELECT COUNT(*) as cnt FROM clientes WHERE empresa_id = ?').get(eid).cnt;
    const totalLotes = db.prepare('SELECT COUNT(*) as cnt FROM lotes_facturacion WHERE empresa_id = ?').get(eid).cnt;

    stats[eid] = { total, pendientes, listos, facturados, montoTotal, montoFacturado, totalClientes, totalLotes };
  }

  res.json(stats);
});

// ── SPA catch-all ────────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(PORT, () => console.log(`[FACTURACION MT-TG] Running on http://localhost:${PORT}`));
