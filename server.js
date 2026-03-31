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

// ── Schema migrations ─────────────────────────────────────────────────────────
try { db.exec('ALTER TABLE movimientos ADD COLUMN lote_carga_id TEXT'); } catch(e){}

// ── Re-classify existing movements on startup using empresa config ─────────────
(function reclassifyMovimientos() {
  try {
    const empresas = getAppData('empresas') || {};
    const movs = db.prepare("SELECT id, empresa_id, rut_normalizado, nombre_origen, estado, tipo_dte FROM movimientos WHERE estado NOT IN ('facturado')").all();
    if (!movs.length) return;
    const upd = db.prepare("UPDATE movimientos SET tipo_dte=?, estado=?, razon_social=COALESCE(NULLIF(razon_social,''), nombre_origen, razon_social), updated_at=? WHERE id=?");
    const now = nowCL ? nowCL() : new Date().toISOString();
    let fixed = 0;
    db.transaction(() => {
      for (const m of movs) {
        if (!m.rut_normalizado) continue;
        const empConfig = empresas[m.empresa_id];
        const correcto = getTipoDte(m.rut_normalizado, empConfig);
        if (m.tipo_dte !== correcto) {
          const nuevoEstado = correcto === 41 ? 'listo' : (m.estado === 'listo' ? 'pendiente' : m.estado);
          upd.run(correcto, nuevoEstado, now, m.id);
          fixed++;
        }
      }
    })();
    if (fixed > 0) console.log(`[MIGRATE] Re-clasificados ${fixed} movimientos con tipo_dte incorrecto`);
  } catch(e) { console.error('[MIGRATE] Error en reclassify:', e.message); }
})();

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

// ── Password migration: actualizar credenciales y renombrar hturra→admin ─────
(function migratePasswords() {
  try {
    const users = getAppData('users');
    if (!users) return;
    let changed = false;
    // Renombrar hturra → admin si existe
    if (users.hturra && !users.admin) {
      users.admin = { ...users.hturra };
      delete users.hturra;
      changed = true;
      console.log('[MIGRATE] Renombrado usuario hturra → admin');
    }
    // Actualizar hashes de contraseñas
    const newHashes = {
      admin:     '1a36e3204acafe38cf3ef45f0bfdae04d527e1ab2f503d574ad33f0c7d3243dc',
      dbravo:    'f1715b8db3bd44bbae81666c6fa794ed0ea93390536a4724c2695bb7b452fe69',
      strujillo: 'ff54329f276f7364be2e3d36a7c13bac328b13a43be274db933cb56985c3954d'
    };
    for (const [uname, hash] of Object.entries(newHashes)) {
      if (users[uname] && users[uname].passHash !== hash) {
        users[uname].passHash = hash;
        changed = true;
        console.log(`[MIGRATE] Contraseña actualizada para ${uname}`);
      }
    }
    if (changed) {
      db.prepare('INSERT OR REPLACE INTO app_data (key, value) VALUES (?, ?)').run('users', JSON.stringify(users));
    }
  } catch(e) { console.error('[MIGRATE] Error en migratePasswords:', e.message); }
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
  const { estado, fecha_desde, fecha_hasta, lote_id, tipo_dte, banco, limit: lim, offset: off } = req.query;
  let sql = 'SELECT * FROM movimientos WHERE 1=1';
  const params = [];

  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  if (estado) { sql += ' AND estado = ?'; params.push(estado); }
  if (tipo_dte) { sql += ' AND tipo_dte = ?'; params.push(parseInt(tipo_dte)); }
  if (banco) { sql += ' AND banco_cartola = ?'; params.push(banco); }
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

  // Generate unique ID for this cartola upload batch
  const loteCargaId = `${empresaId}-${banco_cartola}-${Date.now()}`.toLowerCase().replace(/\s/g,'-');

  const checkDup = db.prepare('SELECT id, estado FROM movimientos WHERE id_compuesto = ?');
  const insertMov = db.prepare(`
    INSERT INTO movimientos (empresa_id, id_transferencia, fecha, monto, glosa, rut, rut_normalizado, nombre_origen, banco_origen, banco_cartola, cuenta_origen, id_compuesto, estado, tipo_dte, razon_social, giro, direccion, comuna, ciudad, email_receptor, nombre_item, descripcion_item, precio, monto_exento, monto_total, fecha_carga, created_at, updated_at, lote_carga_id)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

        // Determine DTE type by RUT using empresa config (tipo_dte_personas / tipo_dte_empresas)
        if (rutNorm) {
          const empConf = getAppData('empresas')?.[empresaId];
          tipoDte = getTipoDte(rutNorm, empConf);

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
          } else {
            // Factura/DTE que no sea boleta sin cliente en BD: usar nombre_origen como razon_social
            // El movimiento queda pendiente para revisión manual (completar datos)
            razonSocial = (mov.nombre_origen || '').substring(0, 100);
          }
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
          now, now, now, loteCargaId
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

// ── Historial de cargas de cartola ───────────────────────────────────────────
app.get('/api/cartolas/historial', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  let sql = `SELECT lote_carga_id, empresa_id, banco_cartola, fecha_carga,
    COUNT(*) as cantidad, SUM(monto) as monto_total
    FROM movimientos WHERE lote_carga_id IS NOT NULL AND lote_carga_id != ''`;
  const params = [];
  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  sql += ' GROUP BY lote_carga_id ORDER BY fecha_carga DESC LIMIT 50';
  res.json(db.prepare(sql).all(...params));
});

app.delete('/api/cartolas/:lote_carga_id', requireAuth, (req, res) => {
  const { lote_carga_id } = req.params;
  const empresaId = filterByEmpresa(req);
  // Only allow deletion of movements not yet in a facturación lote
  let sql = "DELETE FROM movimientos WHERE lote_carga_id = ? AND estado NOT IN ('facturado','en_lote')";
  const params = [lote_carga_id];
  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  const result = db.prepare(sql).run(...params);
  res.json({ ok: true, eliminados: result.changes });
});

// Helper: determinar tipo_dte según RUT y config de empresa
// Default: 34 (Factura Exenta) para TODOS — tipo 41 solo si está explícitamente configurado
// (SF requiere que el tipo 41 tenga plantilla activa configurada — si no existe usar 34)
function getTipoDte(rutNormalizado, empresaConfig) {
  if (!rutNormalizado) return 34;
  const rutNum = parseInt(rutNormalizado.slice(0, -1));
  const sf = empresaConfig?.simplefactura || {};
  if (rutNum >= 76000000) {
    // Empresa/Persona jurídica → Factura Exenta (34) por defecto
    return parseInt(sf.tipo_dte_empresas) || 34;
  } else {
    // Persona natural → default 41 (Boleta Exenta), configurable por empresa
    // IMPORTANTE: TG usa DTE 41, MT puede usar DTE 34 — configurar en cada empresa
    return parseInt(sf.tipo_dte_personas) || 41;
  }
}

// ── Re-classify all pending movements (manual trigger) ───────────────────────
app.post('/api/movimientos/reclasificar', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  const empresas = getAppData('empresas') || {};
  // Incluir 'en_lote' para poder reclasificar movimientos en lotes con error
  let sql = "SELECT id, empresa_id, rut_normalizado, nombre_origen, estado FROM movimientos WHERE estado NOT IN ('facturado')";
  const params = [];
  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  const movs = db.prepare(sql).all(...params);
  const now = nowCL();
  let fixed = 0;
  db.transaction(() => {
    for (const m of movs) {
      if (!m.rut_normalizado) continue;
      const empConfig = empresas[m.empresa_id];
      const correcto = getTipoDte(m.rut_normalizado, empConfig);
      // Estado: si es boleta (41) → listo (no requiere revisión), si es factura → mantener estado actual
      const nuevoEstado = correcto === 41 ? 'listo' : (m.estado === 'listo' ? 'pendiente' : m.estado);
      db.prepare('UPDATE movimientos SET tipo_dte=?, estado=?, updated_at=? WHERE id=?')
        .run(correcto, nuevoEstado, now, m.id);
      fixed++;
    }
  })();
  res.json({ ok: true, reclasificados: fixed });
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

// Detalle de movimientos de un lote
app.get('/api/facturacion/lotes/:lote_id/movimientos', requireAuth, (req, res) => {
  const movs = db.prepare(
    `SELECT id, fecha, monto, monto_total, rut, razon_social, nombre_origen, tipo_dte,
            estado, banco_cartola, email_receptor, giro, direccion, comuna, ciudad,
            id_transferencia, id_compuesto
     FROM movimientos WHERE lote_id = ? ORDER BY id`
  ).all(req.params.lote_id);
  res.json(movs);
});

// ── SimpleFactura helpers ─────────────────────────────────────────────────────
// API pública documentada: https://documentacion.simplefactura.cl/
const SF_API  = 'https://api.simplefactura.cl';
// API backend (endpoints internos que no están en la doc pública pero aún funcionan)
const SF_BASE = 'https://backend.simplefactura.cl/api';

// Construye un cuerpo multipart/form-data manualmente como Buffer (más confiable
// que el FormData nativo de Node.js que puede fallar con Blob en fetch)
function buildMultipartBody(fileBuffer, fieldName, filename, mimeType, extraFields = {}) {
  const boundary = 'FacturaBoundary' + Date.now().toString(16) + Math.random().toString(16).slice(2, 10);
  const CRLF = '\r\n';
  const parts = [];

  // Campos de texto adicionales (p. ej. solicitudString)
  for (const [name, value] of Object.entries(extraFields)) {
    parts.push(Buffer.from(`--${boundary}${CRLF}Content-Disposition: form-data; name="${name}"${CRLF}${CRLF}${value}${CRLF}`, 'utf8'));
  }

  // Archivo
  const fileHeader = `--${boundary}${CRLF}Content-Disposition: form-data; name="${fieldName}"; filename="${filename}"${CRLF}Content-Type: ${mimeType}${CRLF}${CRLF}`;
  parts.push(Buffer.from(fileHeader, 'utf8'));
  parts.push(fileBuffer);
  parts.push(Buffer.from(`${CRLF}--${boundary}--${CRLF}`, 'utf8'));

  const body = Buffer.concat(parts);
  return { body, contentType: `multipart/form-data; boundary=${boundary}` };
}

// Decodifica el payload de un JWT de SimpleFactura (sin verificar firma)
function sfDecodeJwt(token) {
  try {
    return JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString('utf8'));
  } catch(e) { return {}; }
}

// Cache de tokens por empresa (email → { token, expiresAt })
const sfTokenCache = {};

function sfTokenFromCache(email) {
  const cached = sfTokenCache[email];
  if (!cached) return null;
  // JWT exp claim: decode payload to check expiry (subtract 5 min margin)
  try {
    const payload = JSON.parse(Buffer.from(cached.token.split('.')[1], 'base64').toString());
    if (payload.exp && Date.now() / 1000 < payload.exp - 300) return cached.token;
  } catch(e) {
    // If can't decode, use time-based cache (23h)
    if (Date.now() < cached.expiresAt) return cached.token;
  }
  delete sfTokenCache[email];
  return null;
}

// Obtener token SF usando login email/password (el "api_token" de SF Integraciones
// es para webhooks salientes, NO para autenticar llamadas REST → se ignora aquí)
async function sfGetToken(email, password, _apiTokenIgnored) {
  // 1. Return cached token if still valid (login previo)
  const cached = sfTokenFromCache(email);
  if (cached) {
    console.log(`[SF TOKEN] Usando token en caché para ${email}`);
    return cached;
  }

  // 2. Fresh login — usar API pública documentada: POST /token con {email, password}
  const doLogin = async () => {
    console.log(`[SF LOGIN] POST ${SF_API}/token para ${email}`);
    const r = await fetch(`${SF_API}/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password })
    });
    const raw = await r.text();
    console.log(`[SF LOGIN] HTTP ${r.status} → ${raw.substring(0, 400)}`);
    let data;
    try { data = JSON.parse(raw); } catch(e) { throw new Error(`Login respuesta no-JSON (HTTP ${r.status}): ${raw.substring(0, 200)}`); }
    return { ok: r.ok, data, status: r.status };
  };

  let { ok, data, status } = await doLogin();

  // La API pública /token no tiene el problema de "sesión activa" del backend viejo,
  // pero manejamos errores generales de todas formas
  if (!ok && JSON.stringify(data || '').toLowerCase().includes('activa')) {
    console.log('[SF LOGIN] Sesión activa detectada, reintentando en 3s...');
    await new Promise(r => setTimeout(r, 3000));
    ({ ok, data, status } = await doLogin());
  }

  // La API pública devuelve accessToken (camelCase); manejar variantes por si acaso
  const accessToken = data?.accessToken || data?.access_token || data?.token;
  if (!accessToken) {
    const errors = data?.errors;
    const errDetail = Array.isArray(errors) ? errors.join('. ') : (typeof errors === 'object' ? JSON.stringify(errors) : errors);
    const errMsg = errDetail || data?.message || data?.data || JSON.stringify(data);
    throw new Error(`Login fallido (HTTP ${status}): ${errMsg}`);
  }

  // Guardar en caché (token dura 24h según docs, cacheamos 23h)
  sfTokenCache[email] = { token: accessToken, expiresAt: Date.now() + 23 * 60 * 60 * 1000 };
  console.log(`[SF TOKEN] Nuevo token guardado en caché para ${email} (${accessToken.substring(0,20)}...)`);
  return accessToken;
}

// Mantener compatibilidad con código existente
async function sfLogin(email, password) {
  return sfGetToken(email, password);
}

// Obtener sucursalId + emisorId desde SimpleFactura
// rutEmisorSF: RUT del emisor SF (ej. "77859376-9") — clave para cuentas multi-RUT
// manualSucursalId / manualEmisorId: IDs manuales configurados en la UI (bypass del lookup automático)
// Retorna { sucursalId, emisorId } y actualiza sfTokenCache[email]
async function sfGetSucursalId(email, password, nombreSucursal, rutEmisorSF, manualSucursalId, manualEmisorId) {
  // ── Atajo: IDs manuales configurados → usarlos directamente sin llamar a la API ──────────
  if (manualSucursalId && manualEmisorId) {
    console.log(`[SF SUCURSAL] ✓ Usando IDs manuales configurados: sucursalId=${manualSucursalId.substring(0,8)}..., emisorId=${manualEmisorId.substring(0,8)}...`);
    const token = sfTokenCache[email]?.token || await sfGetToken(email, password);
    if (sfTokenCache[email]) {
      sfTokenCache[email].sucursalId = manualSucursalId;
      sfTokenCache[email].emisorId   = manualEmisorId;
    }
    return manualSucursalId;
  }

  const token = sfTokenCache[email]?.token || await sfGetToken(email, password);

  // ── Paso 0: Extraer emisorId directamente desde los claims del JWT ──────────
  // Para cuentas directas (no-reseller) el JWT lleva el emisorId del dueño de la cuenta.
  const jwtClaims = sfDecodeJwt(token);
  // SF puede usar distintos nombres de campo según la versión de la API
  // IMPORTANTE: SF usa "nameid" (claim estándar .NET) como el EmisorId del emisor autenticado
  const jwtEmisorId = jwtClaims?.EmisorId || jwtClaims?.emisorId
    || jwtClaims?.Emisor_Id || jwtClaims?.emisor_id
    || jwtClaims?.IdEmisor  || jwtClaims?.id_emisor
    || jwtClaims?.nameid    || null;  // <-- nameid es el emisorId en JWT de SimpleFactura
  console.log(`[SF JWT] claims keys: ${Object.keys(jwtClaims).join(', ')}`);
  console.log(`[SF JWT] nameid: ${jwtClaims?.nameid}, jwtEmisorId resuelto: ${jwtEmisorId}`);

  try {
    // ── Paso 1: Intentar obtener las sucursales del emisor del JWT primero ─────
    // Esto es clave para cuentas directas donde el dueño NO aparece en la lista general.
    let lista = [];

    if (jwtEmisorId) {
      const respJwt = await fetch(`${SF_BASE}/Sucursal/list/filter?EmisorId=${jwtEmisorId}`, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      });
      if (respJwt.ok) {
        const rawJwt = await respJwt.text();
        const bodyJwt = JSON.parse(rawJwt);
        const listaJwt = Array.isArray(bodyJwt) ? bodyJwt : (Array.isArray(bodyJwt?.data) ? bodyJwt.data : []);
        if (listaJwt.length > 0) {
          console.log(`[SF SUCURSAL] ${listaJwt.length} sucursales del emisor JWT (EmisorId=${jwtEmisorId.substring(0,8)}...):`);
          listaJwt.forEach(s => console.log(`  "${s.nombre||s.Nombre}" | rutEmisor: ${s.rutEmisor||s.RutEmisor||''} | sucursalId: ${(s.sucursalId||s.SucursalId||'').substring(0,8)}...`));
          lista = listaJwt;
        }
      }
    }

    // ── Paso 2: Si no encontramos nada con el JWT, buscar en la lista general ──
    if (lista.length === 0) {
      const resp = await fetch(`${SF_BASE}/Sucursal/list/filter`, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      });
      const raw = await resp.text();
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${raw.substring(0, 200)}`);
      const body = JSON.parse(raw);
      lista = Array.isArray(body) ? body : (Array.isArray(body?.data) ? body.data : []);
      console.log(`[SF SUCURSAL] ${lista.length} sucursales en lista general:`);
      lista.forEach(s => console.log(`  "${s.nombre||s.Nombre}" | emisor: "${s.emisorNombre||s.EmisorNombre||''}" | rutEmisor: ${s.rutEmisor||s.RutEmisor||''} | emisorId: ${(s.emisorId||s.EmisorId||'').substring(0,8)}... | sucursalId: ${(s.sucursalId||s.SucursalId||'').substring(0,8)}...`));
    }

    let match = null;

    // 1. Match por RUT del emisor SF (campo rut_emisor_sf en config)
    if (rutEmisorSF) {
      const rutLimpio = rutEmisorSF.replace(/[.\-]/g, '').toLowerCase();
      match = lista.find(s => {
        const rut = ((s.rutEmisor || s.RutEmisor || '').toString()).replace(/[.\-]/g, '').toLowerCase();
        const nombre = (s.emisorNombre || s.EmisorNombre || '').toLowerCase();
        return rut === rutLimpio || nombre.replace(/[.\-\s]/g,'').toLowerCase().includes(rutLimpio);
      });
      if (match) console.log(`[SF SUCURSAL] ✓ Match por RUT "${rutEmisorSF}" → emisor "${match.emisorNombre||match.EmisorNombre}"`);
    }

    // 2. Match por emisorId del JWT en la lista general (para cuentas directas)
    if (!match && jwtEmisorId) {
      match = lista.find(s => (s.emisorId || s.EmisorId || '') === jwtEmisorId);
      if (match) console.log(`[SF SUCURSAL] ✓ Match por JWT emisorId en lista general`);
    }

    // 3. Match por nombre de sucursal
    if (!match && nombreSucursal) {
      const nombreBuscar = nombreSucursal.toLowerCase().trim();
      match = lista.find(s => (s.nombre || s.Nombre || '').toLowerCase().trim() === nombreBuscar);
      if (match) console.log(`[SF SUCURSAL] ✓ Match por nombre sucursal "${nombreSucursal}"`);
    }

    // 4. Fallback: primera activa
    if (!match) {
      match = lista.find(s => s.activa !== false) || lista[0] || null;
      if (match) console.log(`[SF SUCURSAL] ⚠ Fallback a primera sucursal: "${match.nombre||match.Nombre}" (emisor: "${match.emisorNombre||match.EmisorNombre}")`);
    }

    if (!match && !jwtEmisorId) throw new Error('No se encontraron sucursales en SimpleFactura');

    const uuid = match
      ? (match.sucursalId || match.SucursalId)
      : null;

    // Prioridad emisorId: 1) desde match en lista, 2) desde JWT claims
    const emisorId = (match ? (match.emisorId || match.EmisorId || null) : null) || jwtEmisorId || null;
    console.log(`[SF SUCURSAL] → sucursalId: ${uuid}, emisorId: ${emisorId} (fuente: ${match ? 'lista' : 'JWT'})`);

    // Actualizar cache con los valores correctos para este emisor
    if (sfTokenCache[email]) {
      sfTokenCache[email].sucursalId = uuid;
      sfTokenCache[email].emisorId   = emisorId;
    }

    return uuid;
  } catch (err) {
    console.error('[SF SUCURSAL] Error al obtener sucursalId:', err.message);
    return null;
  }
}

// ── CSV helper (formato oficial SimpleFactura, semicolon-separated con BOM) ───
// Formato oficial SF: exactamente 39 columnas — NO agregar columnas extra aquí
// Formato CSV de ejemplo descargado desde app.simplefactura.cl → Ventas → Factura Masiva → Ejemplos CSV
// 39 columnas oficiales + 3 columnas de tracking interno (ID Transferencia, Cartola, Id Compuesto)
const SF_CSV_HEADERS = [
  'Id','TipoDte','FmaPago','FechaEmision','Vencimiento','RutRecep','GiroRecep','Contacto','CorreoRecep',
  'DirRecep','CmnaRecep','CiudadRecep','RazonSocialRecep','DirDest','CmnaDest','CiudadDest',
  'ReferenciaTpoDocRef','ReferenciaFolioRef','ReferenciaFchRef','ReferenciaRazonRef','ReferenciaCodigo',
  'CodigoProducto','NombreProducto','DescripcionProducto','CantidadProducto','PrecioProducto',
  'UnidadMedidaProducto','DescuentoProducto','RecargoProducto','RebajaAvaluo','IndicadorExento',
  'TotalProducto','GlosaDR','TpoMov','TpoValor','ValorDR','ValorOtrMnda','IndExeDR','Correo',
  'ID Transferencia','Cartola','Id Compuesto'
];

function rutParaSF(rut) {
  // Formato SimpleFactura CSV: sin puntos, con guión (ej: 77653656-3)
  if (!rut) return '';
  return rut.replace(/\./g, ''); // quitar puntos, mantener guión
}

// Formato RUT con puntos para Credenciales (registro interno SimpleFactura: "77.859.376-9")
function rutConPuntos(rut) {
  if (!rut) return '';
  const clean = rut.replace(/\./g, ''); // asegurar sin puntos primero
  const dash = clean.lastIndexOf('-');
  if (dash < 0) return rut; // sin guión, devolver tal cual
  const num = clean.substring(0, dash);
  const dv  = clean.substring(dash + 1);
  // insertar punto cada 3 dígitos desde la derecha
  const formatted = num.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return `${formatted}-${dv}`;
}

function fechaParaSF(fechaISO) {
  // DB guarda YYYY-MM-DD, SimpleFactura espera DD-MM-YYYY
  if (!fechaISO) return todayCL().split('-').reverse().join('-');
  const parts = fechaISO.split('-');
  if (parts.length === 3) return `${parts[2]}-${parts[1]}-${parts[0]}`;
  return fechaISO; // si ya está en otro formato, devolver tal cual
}

function escaparCsvSF(val) {
  const s = String(val ?? '');
  // Envolver en comillas si contiene punto y coma o comillas
  if (s.includes(';') || s.includes('"')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function buildSfCsvRows(movs, empresa) {
  return movs.map((m, i) => {
    const fecha = fechaParaSF(m.fecha);
    const correoRecep = m.email_receptor || empresa.email_facturacion || '';
    // campo Correo (col 38): solo si el receptor tiene email propio
    const correoExtra = m.email_receptor || '';
    // Campos del receptor — basados en el formato oficial de los CSVs de ejemplo
    // GiroRecep, CiudadRecep pueden ir vacíos (SF los acepta); sólo RazonSocial y DirRecep requieren valor
    const razonSocial = (m.razon_social || m.nombre_origen || '').substring(0, 100);
    const giro        = (m.giro || '').substring(0, 80);
    const direccion   = (m.direccion || 'NO INFORMADO').substring(0, 100);
    const comuna      = m.comuna || '';
    const ciudad      = m.ciudad || '';
    // tipo_dte: siempre derivar desde config de empresa (ignora valor guardado en movimiento)
    // Esto permite que un cambio de config se aplique inmediatamente al re-emitir un lote
    const tipoDte = getTipoDte(m.rut_normalizado, empresa);

    return [
      i + 1,
      tipoDte,
      1,                        // FmaPago: contado
      fecha,                    // FechaEmision DD-MM-YYYY
      fecha,                    // Vencimiento (igual a emisión)
      rutParaSF(m.rut),         // RutRecep sin puntos con guión
      giro,
      'NO INFORMADO',           // Contacto
      correoRecep,              // CorreoRecep
      direccion,
      comuna,
      ciudad,
      razonSocial,
      '', '', '',               // DirDest, CmnaDest, CiudadDest
      '', '', '', '', '',       // Referencias (vacías)
      '',                       // CodigoProducto
      m.nombre_item || 'Venta paquete activo digital',
      m.descripcion_item || `Venta paquete activo digital Banco ${m.banco_cartola}`,
      1,                        // CantidadProducto
      m.monto_total || 0,       // PrecioProducto
      'UNID',
      0, 0, 0,                  // Descuento, Recargo, RebajaAvaluo
      1,                        // IndicadorExento=1 (tipos 34 y 41 son AMBOS exentos)
      m.monto_total || 0,       // TotalProducto
      '', '', '', '', '', '',   // GlosaDR, TpoMov, TpoValor, ValorDR, ValorOtrMnda, IndExeDR
      correoExtra,              // Correo (col 39)
      m.id_transferencia || '', // ID Transferencia (col 40 — tracking)
      m.banco_cartola || '',    // Cartola (col 41 — tracking)
      m.id_compuesto || ''      // Id Compuesto (col 42 — tracking)
    ];
  });
}

function buildSfCsvContent(movs, empresa) {
  let csv = SF_CSV_HEADERS.join(';') + '\n';
  for (const row of buildSfCsvRows(movs, empresa)) {
    csv += row.map(escaparCsvSF).join(';') + '\n';
  }
  return '\uFEFF' + csv; // BOM UTF-8 para compatibilidad Excel/SimpleFactura
}

// Alias para compatibilidad con el endpoint de emisión vía API
function buildSfCsv(movs, empresa) {
  return buildSfCsvContent(movs, empresa);
}

// ── Debug: descargar el CSV que se enviaría a SF sin emitirlo ─────────────────
app.get('/api/facturacion/preview-csv/:lote_id', requireAuth, (req, res) => {
  const loteId = req.params.lote_id;
  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });
  const empresas = getAppData('empresas');
  const empresa = empresas[lote.empresa_id];
  if (!empresa) return res.status(404).json({ error: 'Empresa no encontrada' });
  const movs = db.prepare(`SELECT * FROM movimientos WHERE lote_id = ? AND estado IN ('en_lote','error')`).all(loteId);
  if (!movs.length) return res.status(400).json({ error: 'Sin movimientos en lote' });
  const csv = buildSfCsvContent(movs, empresa);
  const rows = buildSfCsvRows(movs, empresa);
  console.log(`[CSV PREVIEW] lote=${loteId} cols_header=${SF_CSV_HEADERS.length} cols_row=${rows[0]?.length} movs=${movs.length}`);
  console.log(`[CSV PREVIEW] Primera fila: ${JSON.stringify(rows[0])}`);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${loteId}.csv"`);
  res.send(csv);
});

// Emit via SimpleFactura API (CSV upload)
app.post('/api/facturacion/emitir/:lote_id', requireAuth, async (req, res) => {
  const loteId = req.params.lote_id;
  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });

  const empresas = getAppData('empresas');
  const empresa = empresas[lote.empresa_id];
  const sfConf = empresa?.simplefactura || {};
  if (!sfConf.api_token && (!sfConf.username || !sfConf.password)) {
    return res.status(400).json({ error: 'Credenciales de SimpleFactura no configuradas (necesita Token API o email/contraseña)' });
  }

  // Al re-intentar, los movimientos pueden estar en estado 'error' → resetearlos a 'en_lote'
  const nowReset = nowCL();
  const resetResult = db.prepare("UPDATE movimientos SET estado='en_lote', updated_at=? WHERE lote_id=? AND estado IN ('error','listo')").run(nowReset, loteId);
  if (resetResult.changes > 0) console.log(`[EMITIR] ${resetResult.changes} movimientos reseteados a 'en_lote' para reintento`);

  const movs = db.prepare("SELECT * FROM movimientos WHERE lote_id = ? AND estado = 'en_lote'").all(loteId);
  if (movs.length === 0) return res.status(400).json({ error: 'No hay movimientos en este lote (verifique que el lote tenga movimientos asignados)' });

  const now = nowCL();
  const sfConfig = empresa.simplefactura;

  try {
    // 1. Obtener token SF (API token estático tiene prioridad sobre login)
    const token = await sfGetToken(sfConfig.username, sfConfig.password, sfConfig.api_token || null);
    console.log(`[SF] Token obtenido para ${lote.empresa_id} (método: ${sfConfig.api_token ? 'API token' : 'login'})`);

    // 2. Generar CSV en formato SimpleFactura
    const csvContent = buildSfCsv(movs, empresa);
    const csvFilename = `Facturacion_${lote.empresa_id}_${loteId}.csv`;
    const csvBuffer = Buffer.from(csvContent, 'utf8');

    // 3. Subir CSV — API pública documentada: POST https://api.simplefactura.cl/massiveInvoice
    //    Docs: https://documentacion.simplefactura.cl/#aa06de6b-dd1d-4b63-812e-e08f703c9c58
    //    Form fields: "data" (JSON con rutEmisor+nombreSucursal) + "input" (archivo CSV)
    const SF_UPLOAD_URL = `${SF_API}/massiveInvoice`;

    const nombreSucursal = (sfConfig.nombre_sucursal || 'Casa Matriz').trim();
    const rutEmisor      = rutParaSF(sfConfig.rut_emisor || empresa.rut || '');

    // Construir campo "data" según documentación oficial SF
    const dataJson = JSON.stringify({
      rutEmisor: rutEmisor,
      nombreSucursal: nombreSucursal
    });
    console.log(`[SF UPLOAD] URL: ${SF_UPLOAD_URL}`);
    console.log(`[SF UPLOAD] data: ${dataJson}`);
    console.log(`[SF UPLOAD] CSV: ${csvFilename} (${csvBuffer.length} bytes, ${movs.length} movimientos)`);

    let lastDataJson = dataJson;
    const doUpload = async (tok) => {
      // Campos multipart: "data" = JSON credenciales, "input" = archivo CSV
      const { body: mpBody, contentType: mpCT } = buildMultipartBody(
        csvBuffer, 'input', csvFilename, 'text/csv', { data: dataJson }
      );
      return fetch(SF_UPLOAD_URL, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${tok}`, 'Content-Type': mpCT },
        body: mpBody
      });
    };

    let uploadResp = await doUpload(token);

    // Si 401, el token puede haber expirado → refrescar y reintentar una vez
    if (uploadResp.status === 401) {
      console.log('[SF UPLOAD] Token rechazado (401) — refrescando y reintentando...');
      delete sfTokenCache[sfConfig.username];
      const newToken = await sfGetToken(sfConfig.username, sfConfig.password, sfConfig.api_token || null);
      uploadResp = await doUpload(newToken);
    }

    const rawText = await uploadResp.text();
    console.log(`[SF UPLOAD] HTTP ${uploadResp.status} → ${rawText.substring(0, 800)}`);
    let data;
    try { data = JSON.parse(rawText); } catch(e) { data = { raw: rawText, httpStatus: uploadResp.status }; }

    // Respuesta exitosa según docs: { status: 200, message: "...", data: [{idCsv, folio}...], errors: null }
    const isSuccess = uploadResp.ok && data?.status === 200 && !data?.errors;
    // También aceptar formato antiguo por compatibilidad
    const tieneErrores = data?.data?.tieneErrores;
    const loteEstado = (isSuccess || (uploadResp.ok && tieneErrores !== true)) ? 'emitido' : 'error';

    // Mensaje legible
    let mensaje;
    if (loteEstado === 'emitido') {
      if (data?.message) {
        mensaje = data.message;
        // Agregar folios si disponibles
        if (Array.isArray(data.data)) {
          const folios = data.data.map(d => d.folio).filter(Boolean);
          if (folios.length) mensaje += ` | Folios: ${folios.join(', ')}`;
        }
      } else {
        const d = data.data || {};
        mensaje = `Procesado: ${d.cantidadDte || movs.length} DTEs`;
        if (d.montoTotal) mensaje += `, $${d.montoTotal.toLocaleString('es-CL')}`;
      }
    } else {
      const errs = data?.errors;
      const errBody = Array.isArray(errs) ? errs.join('. ')
        : (typeof errs === 'object' && errs !== null ? JSON.stringify(errs) : errs)
        || data?.message || data?.title
        || (data?.raw !== undefined ? `Respuesta vacía del servidor` : null)
        || JSON.stringify(data);
      mensaje = `HTTP ${uploadResp.status}: ${errBody} [data enviada: ${lastDataJson}]`;
    }

    // 4. Si exitoso, marcar movimientos como facturados
    if (loteEstado === 'emitido') {
      const updStmt = db.prepare("UPDATE movimientos SET estado = 'facturado', fecha_facturacion = ?, updated_at = ? WHERE id = ?");
      db.transaction(() => { for (const m of movs) updStmt.run(now, now, m.id); })();
    }

    db.prepare('UPDATE lotes_facturacion SET estado = ?, response_api = ?, updated_at = ? WHERE lote_id = ?')
      .run(loteEstado, JSON.stringify({ httpStatus: uploadResp.status, message: mensaje, data: data?.data }), now, loteId);

    res.json({ ok: loteEstado === 'emitido', estado: loteEstado, mensaje, httpStatus: uploadResp.status });
  } catch (err) {
    console.error('[SIMPLEFACTURA ERROR]', err);
    db.prepare('UPDATE lotes_facturacion SET estado = ?, response_api = ?, updated_at = ? WHERE lote_id = ?')
      .run('error', JSON.stringify({ error: err.message }), now, loteId);
    res.status(500).json({ error: 'Error al conectar con SimpleFactura: ' + err.message });
  }
});

// Test SimpleFactura connectivity
app.get('/api/facturacion/test-sf/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa = empresas[req.params.empresa_id];
  const sfConf = empresa?.simplefactura || {};
  if (!sfConf.api_token && !sfConf.username) return res.status(400).json({ error: 'Credenciales no configuradas' });
  const email = sfConf.username || 'api-token';
  // Invalidar caché para forzar obtención fresca en el test
  delete sfTokenCache[email];
  try {
    const token = await sfGetToken(email, sfConf.password, sfConf.api_token || null);
    const claims = sfDecodeJwt(token);
    const nombreSucursal = (sfConf.nombre_sucursal || 'Casa Matriz').trim();
    const rutEmisorSF_test = (sfConf.rut_emisor_sf || '').trim() || null;
    const manualSucId = (sfConf.sucursal_id_sf || '').trim() || null;
    const manualEmId  = (sfConf.emisor_id_sf  || '').trim() || null;
    // También obtener sucursalId UUID para diagnóstico
    const sucursalUUID = await sfGetSucursalId(email, sfConf.password, nombreSucursal, rutEmisorSF_test, manualSucId, manualEmId);
    const emisorId = sfTokenCache[email]?.emisorId || null;
    let solicitudObj;
    if (sucursalUUID) {
      solicitudObj = { sucursalId: sucursalUUID };
      if (emisorId) solicitudObj.idEmisor = emisorId;
    } else {
      solicitudObj = { Credenciales: { RutEmisor: rutParaSF(empresa.simplefactura?.rut_emisor || ''), NombreSucursal: nombreSucursal } };
    }
    res.json({
      ok: true,
      mensaje: `Login exitoso para ${email}`,
      jwtClaims: claims,
      sucursalUUID: sucursalUUID || 'no encontrado',
      emisorId: emisorId || 'no encontrado',
      solicitudStringQueSeEnviara: JSON.stringify(solicitudObj)
    });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// Helper: obtiene la lista global de TipoPlantilla desde GET /api/Plantilla/list
// Respuesta: PagedResponse { data: [TipoPlantillaEnt], pageNumber, pageSize, ... }
async function sfGetTiposPlantilla(token) {
  const r = await fetch(`${SF_BASE}/Plantilla/list?PageNumber=1&PageSize=200`, {
    method: 'GET',
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const body = await r.text();
  let parsed;
  try { parsed = JSON.parse(body); } catch(e) { return []; }
  // PagedResponse → items en .data
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.data)) return parsed.data;
  return [];
}

// Helper: obtiene PlantillaEmisor del emisor (sin filtro Activo para obtener todas)
async function sfGetPlantillasEmisor(token, emisorId) {
  const r = await fetch(`${SF_BASE}/PlantillaEmisor/list/filter?EmisorId=${emisorId}&PageNumber=1&PageSize=100`, {
    method: 'GET',
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const body = await r.text();
  let parsed;
  try { parsed = JSON.parse(body); } catch(e) { return []; }
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.data)) return parsed.data;
  if (Array.isArray(parsed?.items)) return parsed.items;
  return [];
}

// Diagnóstico de plantillas: lista las plantillas del emisor y resuelve tipos DTE
app.get('/api/facturacion/diagnostico-plantillas/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa  = empresas[req.params.empresa_id];
  const sfConf   = empresa?.simplefactura || {};
  if (!sfConf.username) return res.status(400).json({ error: 'Credenciales no configuradas' });
  const email = sfConf.username;
  try {
    const token = await sfGetToken(email, sfConf.password);
    const nombreSucursal = (sfConf.nombre_sucursal || 'Casa Matriz').trim();
    const rutEmisorSF_diag = (sfConf.rut_emisor_sf || '').trim() || null;
    const manualSucId_diag = (sfConf.sucursal_id_sf || '').trim() || null;
    const manualEmId_diag  = (sfConf.emisor_id_sf  || '').trim() || null;
    await sfGetSucursalId(email, sfConf.password, nombreSucursal, rutEmisorSF_diag, manualSucId_diag, manualEmId_diag);
    const emisorId = sfTokenCache[email]?.emisorId || null;
    if (!emisorId) return res.status(400).json({ error: 'No se pudo obtener emisorId de SF' });

    // 1. Plantillas del emisor
    const plantillasEmisor = await sfGetPlantillasEmisor(token, emisorId);

    // 2. Tipos globales: GET /api/Plantilla/list → PagedResponse { data: [TipoPlantillaEnt] }
    //    TipoPlantillaEnt.tipoPlantillaId = mismo UUID que PlantillaEmisor.tipoPlantillaId
    //    TipoPlantillaEnt.codigoTipoDte   = código DTE entero (33, 34, 39, 41...)
    const tiposPlantilla = await sfGetTiposPlantilla(token);

    // 3. Construir mapa tipoPlantillaId → TipoPlantillaEnt para cruce
    const tipoMap = {};
    for (const t of tiposPlantilla) {
      const tid = t.tipoPlantillaId || t.TipoPlantillaId;
      if (tid) tipoMap[tid] = t;
    }

    // 4. Enriquecer plantillasEmisor con codigoTipoDte
    const plantillasEnriquecidas = plantillasEmisor.map(p => {
      const tid  = p.tipoPlantillaId || p.TipoPlantillaId;
      const tipo = tipoMap[tid];
      return {
        ...p,
        _codigoDte: tipo?.codigoTipoDte ?? null,
        _nombreDte: tipo?.nombreTipoDte ?? tipo?.descripcion ?? null
      };
    });

    const activas      = plantillasEnriquecidas.filter(p => p.activo === true || p.Activo === true);
    const dtesCubiertos = [...new Set(activas.map(p => p._codigoDte).filter(v => v !== null))];

    res.json({
      ok: true,
      emisorId,
      plantillasEmisorTotal: plantillasEnriquecidas.length,
      plantillasActivas: activas.length,
      tiposPlantillaGlobal: tiposPlantilla.length,
      dtesCubiertos,
      tieneTipo34: dtesCubiertos.includes(34),
      tieneTipo33: dtesCubiertos.includes(33),
      plantillasEmisor: plantillasEnriquecidas,
      tiposPlantillaDisponibles: tiposPlantilla.map(t => ({
        tipoPlantillaId: t.tipoPlantillaId,
        codigoTipoDte: t.codigoTipoDte,
        nombreTipoDte: t.nombreTipoDte,
        existente: t.existente
      })),
      raw: { primerPlantilla: JSON.stringify(plantillasEmisor[0] || {}) }
    });
  } catch(e) {
    res.json({ ok: false, error: e.message, stack: e.stack?.substring(0, 500) });
  }
});

// Activar plantillas SF para los tipos DTE pedidos (default: 33 y 34)
app.post('/api/facturacion/activar-plantillas/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa  = empresas[req.params.empresa_id];
  const sfConf   = empresa?.simplefactura || {};
  if (!sfConf.username) return res.status(400).json({ error: 'Credenciales no configuradas' });
  const email = sfConf.username;
  const tiposDte = req.body?.tipos_dte || [33, 34];

  try {
    const token = await sfGetToken(email, sfConf.password);
    const nombreSucursal = (sfConf.nombre_sucursal || 'Casa Matriz').trim();
    const rutEmisorSF_act = (sfConf.rut_emisor_sf || '').trim() || null;
    const manualSucId_act = (sfConf.sucursal_id_sf || '').trim() || null;
    const manualEmId_act  = (sfConf.emisor_id_sf  || '').trim() || null;
    await sfGetSucursalId(email, sfConf.password, nombreSucursal, rutEmisorSF_act, manualSucId_act, manualEmId_act);
    const emisorId = sfTokenCache[email]?.emisorId || null;
    if (!emisorId) return res.status(400).json({ error: 'No se pudo obtener emisorId de SF' });

    // Obtener tipos globales (PagedResponse.data)
    const tiposGlobal = await sfGetTiposPlantilla(token);
    const tipoMap     = {};
    for (const t of tiposGlobal) {
      const tid = t.tipoPlantillaId || t.TipoPlantillaId;
      if (tid) tipoMap[tid] = t;
    }

    // Obtener plantillas del emisor
    const plantillas = await sfGetPlantillasEmisor(token, emisorId);

    // Enriquecer con código DTE
    const enriquecidas = plantillas.map(p => ({
      ...p,
      _codigoDte: tipoMap[p.tipoPlantillaId || p.TipoPlantillaId]?.codigoTipoDte ?? null
    }));

    const resultados = [];

    // Paso 1: activar plantillas inactivas para tipos pedidos
    for (const p of enriquecidas) {
      const codDte = p._codigoDte;
      if (codDte !== null && !tiposDte.includes(codDte)) continue;
      const activo = p.activo === true || p.Activo === true;
      const pid    = p.plantillaEmisorId || p.PlantillaEmisorId;
      const tid    = p.tipoPlantillaId   || p.TipoPlantillaId;
      if (activo) { resultados.push({ paso: 'activate', codDte, pid: pid?.substring(0,8), resultado: 'ya activa' }); continue; }
      const body = JSON.stringify({ plantillaEmisorId: pid, emisorId, tipoPlantillaId: tid, activo: true, nombrePlantilla: p.nombrePlantilla || '' });
      const r = await fetch(`${SF_BASE}/PlantillaEmisor/activate`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body
      });
      const rBody = await r.text();
      resultados.push({ paso: 'activate', codDte, pid: pid?.substring(0,8), http: r.status, respuesta: rBody.substring(0, 300) });
    }

    // Paso 2: asignar tipos faltantes usando /assign?emisorId=...&plantillaId=...&activate=true
    // IMPORTANTE: /api/PlantillaEmisor/assign usa QUERY PARAMS (no JSON body)
    const dtesYaCubiertos = new Set(enriquecidas.filter(p => p.activo === true || p.Activo === true).map(p => p._codigoDte).filter(Boolean));
    for (const tipoDte of tiposDte) {
      if (dtesYaCubiertos.has(tipoDte)) { resultados.push({ paso: 'assign', codDte: tipoDte, resultado: 'ya cubierto' }); continue; }
      const tipoGlobal = tiposGlobal.find(t => t.codigoTipoDte === tipoDte);
      if (!tipoGlobal) {
        resultados.push({ paso: 'assign', codDte: tipoDte, resultado: `tipo DTE ${tipoDte} no encontrado en plantillas globales (${tiposGlobal.length} disponibles)` });
        continue;
      }
      const plantillaId = tipoGlobal.tipoPlantillaId || tipoGlobal.TipoPlantillaId;
      // Usar query params tal como especifica el Swagger
      const url = `${SF_BASE}/PlantillaEmisor/assign?emisorId=${emisorId}&plantillaId=${plantillaId}&activate=true`;
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      });
      const rBody = await r.text();
      resultados.push({ paso: 'assign', codDte: tipoDte, plantillaId: plantillaId?.substring(0,8), http: r.status, respuesta: rBody.substring(0, 400) });
    }

    res.json({ ok: true, emisorId, tiposGlobal: tiposGlobal.length, plantillasEmisor: plantillas.length, resultados });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// Listar todos los emisores/sucursales disponibles en la cuenta SF (útil para cuentas multi-RUT)
app.get('/api/facturacion/listar-emisores/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa  = empresas[req.params.empresa_id];
  const sfConf   = empresa?.simplefactura || {};
  if (!sfConf.username) return res.status(400).json({ error: 'Credenciales no configuradas' });
  const email = sfConf.username;
  try {
    const token = await sfGetToken(email, sfConf.password);
    const resp = await fetch(`${SF_BASE}/Sucursal/list/filter`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const raw  = await resp.text();
    const body = JSON.parse(raw);
    const lista = Array.isArray(body) ? body : (Array.isArray(body?.data) ? body.data : []);
    const sucursales = lista.map(s => ({
      emisorNombre:   s.emisorNombre  || s.EmisorNombre  || s.nombre || s.Nombre || '',
      rutEmisor:      s.rutEmisor     || s.RutEmisor     || '',
      nombreSucursal: s.nombre        || s.Nombre        || '',
      sucursalId:     s.sucursalId    || s.SucursalId    || '',
      emisorId:       s.emisorId      || s.EmisorId      || '',
      activa:         s.activa !== false
    }));
    res.json({ ok: true, sucursales, rawPrimer: lista[0] ? JSON.stringify(lista[0]).substring(0, 800) : '' });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// Diagnóstico: muestra los claims del JWT de SimpleFactura (útil para depurar solicitudString)
app.get('/api/facturacion/debug-jwt/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa = empresas[req.params.empresa_id];
  if (!empresa?.simplefactura?.username) return res.status(400).json({ error: 'Credenciales no configuradas' });
  const email = empresa.simplefactura.username;
  try {
    const token = await sfGetToken(email, empresa.simplefactura.password);
    const claims = sfDecodeJwt(token);
    res.json({ ok: true, claims, tokenPreview: token.substring(0, 40) + '...' });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// ── Historial de facturación ─────────────────────────────────────────────────
app.get('/api/facturacion/historial', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  const { buscar, fecha_desde, fecha_hasta, tipo_dte, limit: lim, offset: off } = req.query;
  const pageLimit = Math.min(parseInt(lim) || 100, 500);
  const pageOffset = parseInt(off) || 0;

  let sql = `SELECT id, empresa_id, fecha, monto, monto_total, rut, rut_normalizado,
             razon_social, nombre_origen, tipo_dte, estado, banco_cartola, email_receptor,
             giro, direccion, comuna, ciudad, lote_id, fecha_facturacion, id_transferencia
             FROM movimientos WHERE estado = 'facturado'`;
  const params = [];

  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  if (fecha_desde) { sql += ' AND fecha >= ?'; params.push(fecha_desde); }
  if (fecha_hasta) { sql += ' AND fecha <= ?'; params.push(fecha_hasta); }
  if (tipo_dte) { sql += ' AND tipo_dte = ?'; params.push(parseInt(tipo_dte)); }

  // Buscador: por nombre, RUT o monto
  if (buscar) {
    const term = buscar.trim();
    // Detectar si es búsqueda por monto (solo números y puntos/comas)
    if (/^[\d.,]+$/.test(term)) {
      const montoNum = parseFloat(term.replace(/\./g, '').replace(',', '.'));
      if (!isNaN(montoNum)) {
        sql += ' AND (monto_total = ? OR monto = ?)';
        params.push(montoNum, montoNum);
      }
    } else {
      sql += ` AND (razon_social LIKE ? OR nombre_origen LIKE ? OR rut LIKE ? OR rut_normalizado LIKE ?)`;
      const like = `%${term}%`;
      params.push(like, like, like, like);
    }
  }

  // Count total
  const countSql = sql.replace(/SELECT .+ FROM/, 'SELECT COUNT(*) as total FROM');
  const total = db.prepare(countSql).get(...params)?.total || 0;

  sql += ' ORDER BY fecha_facturacion DESC, id DESC LIMIT ? OFFSET ?';
  params.push(pageLimit, pageOffset);

  const movs = db.prepare(sql).all(...params);

  // Estadísticas resumen
  const statsSql = sql.replace(/SELECT .+ FROM/, 'SELECT COUNT(*) as cnt, SUM(monto_total) as suma FROM')
    .replace(/ORDER BY .+$/, '');
  // Re-run without limit/offset for stats
  const statsParams = params.slice(0, -2); // remove limit and offset
  const stats = db.prepare(
    `SELECT COUNT(*) as cnt, COALESCE(SUM(monto_total),0) as suma FROM movimientos WHERE estado = 'facturado'`
    + (empresaId ? ' AND empresa_id = ?' : '')
  ).get(...(empresaId ? [empresaId] : []));

  res.json({ movimientos: movs, total, stats });
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

  const csv = buildSfCsvContent(movs, empresa);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Facturacion_${lote?.empresa_id}_${loteId}.csv"`);
  res.send(csv);
});

// Export movimientos as CSV (without lote - for manual selection)
app.post('/api/facturacion/exportar-seleccion', requireAuth, (req, res) => {
  const { movimiento_ids, empresa_id } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  const placeholders = movimiento_ids.map(() => '?').join(',');
  const movs = db.prepare(`SELECT * FROM movimientos WHERE id IN (${placeholders})`).all(...movimiento_ids);

  const empresas = getAppData('empresas');
  const empresa = empresas[empresaId] || {};

  const csv = buildSfCsvContent(movs, empresa);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Facturacion_${empresaId}_seleccion.csv"`);
  res.send(csv);
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
