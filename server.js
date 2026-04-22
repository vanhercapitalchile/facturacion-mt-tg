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
try { db.exec('ALTER TABLE lotes_facturacion ADD COLUMN nombre TEXT'); } catch(e){}
try { db.exec('ALTER TABLE movimientos ADD COLUMN cargado_por TEXT'); } catch(e){}

// ── Migration: limpiar CR/LF en campos de texto (emails con newline al final) ─
// Causa de error SF: "Field at index N does not exist" cuando un email tiene \n
try {
  const cleaned = db.prepare(`UPDATE movimientos SET
    email_receptor = TRIM(REPLACE(REPLACE(email_receptor, char(13), ''), char(10), '')),
    nombre_origen  = TRIM(REPLACE(REPLACE(nombre_origen,  char(13), ''), char(10), '')),
    razon_social   = TRIM(REPLACE(REPLACE(razon_social,   char(13), ''), char(10), '')),
    giro           = TRIM(REPLACE(REPLACE(giro,           char(13), ''), char(10), ''))
    WHERE email_receptor LIKE '%' || char(10) || '%'
       OR email_receptor LIKE '%' || char(13) || '%'
       OR nombre_origen  LIKE '%' || char(10) || '%'
       OR razon_social   LIKE '%' || char(10) || '%'
       OR giro           LIKE '%' || char(10) || '%'`).run();
  if (cleaned.changes > 0) console.log(`[MIGRATION] Limpiados CR/LF en ${cleaned.changes} movimientos`);
} catch(e) { console.warn('[MIGRATION] Error limpiando CR/LF en movimientos:', e.message); }
try {
  const cleanedCli = db.prepare(`UPDATE clientes SET
    email = TRIM(REPLACE(REPLACE(email, char(13), ''), char(10), '')),
    razon_social = TRIM(REPLACE(REPLACE(razon_social, char(13), ''), char(10), ''))
    WHERE email LIKE '%' || char(10) || '%'
       OR email LIKE '%' || char(13) || '%'`).run();
  if (cleanedCli.changes > 0) console.log(`[MIGRATION] Limpiados CR/LF en ${cleanedCli.changes} clientes`);
} catch(e) { console.warn('[MIGRATION] Error limpiando CR/LF en clientes:', e.message); }

// ── Migration: Santander id_compuesto → incluir fecha para evitar duplicados entre meses ─
// Santander usa un contador secuencial (1,2,3...) que se reinicia cada período.
// Sin la fecha, "1_SANTANDER" de marzo bloquea "1_SANTANDER" de abril.
try {
  const staleRows = db.prepare(`
    SELECT id, fecha, id_transferencia
    FROM movimientos
    WHERE banco_cartola = 'SANTANDER'
    AND id_compuesto NOT LIKE '%____-__-__%'
  `).all();
  if (staleRows.length > 0) {
    const updateRow = db.prepare(`UPDATE movimientos SET id_transferencia = ?, id_compuesto = ? WHERE id = ?`);
    let migrated = 0;
    for (const row of staleRows) {
      const fechaPrefix = (row.fecha || '').substring(0, 10); // YYYY-MM-DD
      if (!fechaPrefix || fechaPrefix.length < 10) continue;
      try {
        updateRow.run(`${fechaPrefix}_${row.id_transferencia}`, `${fechaPrefix}_${row.id_transferencia}_SANTANDER`, row.id);
        migrated++;
      } catch(innerErr) {
        console.warn(`[MIGRATION] Santander ID skip id=${row.id}: ${innerErr.message}`);
      }
    }
    console.log(`[MIGRATION] Santander IDs migrados con fecha: ${migrated}/${staleRows.length}`);
  }
} catch(e) { console.warn('[MIGRATION] Error en migración Santander IDs:', e.message); }

// ── Migration: prefixar id_compuesto con empresa_id para unicidad per-empresa ──
// Evita que la constraint UNIQUE global bloquee IDs idénticos entre empresas distintas.
// Seguro de correr múltiples veces: omite filas cuyo id_compuesto ya tiene el prefijo.
try {
  const rows = db.prepare(`
    SELECT id, empresa_id, id_compuesto
    FROM movimientos
    WHERE id_compuesto IS NOT NULL AND id_compuesto != ''
      AND id_compuesto NOT LIKE (empresa_id || '_%')
  `).all();
  if (rows.length > 0) {
    const stmtPfx = db.prepare(`UPDATE movimientos SET id_compuesto = ? WHERE id = ?`);
    let pfxMigrated = 0, pfxSkipped = 0;
    for (const row of rows) {
      const newId = `${row.empresa_id}_${row.id_compuesto}`;
      try { stmtPfx.run(newId, row.id); pfxMigrated++; }
      catch(e) { pfxSkipped++; }
    }
    console.log(`[MIGRATION] id_compuesto empresa-prefix: ${pfxMigrated} migrados, ${pfxSkipped} omitidos`);
  }
} catch(e) { console.warn('[MIGRATION] Error prefijando id_compuesto con empresa:', e.message); }

// ── RUTs internos excluidos de facturación (transferencias entre empresas propias) ─
const RUTS_INTERNOS = [
  '778593769', '778856980', '775063432', '779766063', // TG / MT / TS Capital / Vanher Capital
  '167938582'  // Hernán Ariel Turra Guzmán — no facturable en ninguna empresa
];
try {
  db.exec(`UPDATE movimientos SET estado='interno', updated_at=datetime('now')
           WHERE rut_normalizado IN ('778593769','778856980','775063432','779766063','167938582')
           AND estado IN ('pendiente','listo')`);
  console.log('[STARTUP] RUTs internos marcados como interno');
} catch(e) { console.warn('[STARTUP] Error marcando RUTs internos:', e.message); }

// ── Nombres internos — coincidencia por nombre cuando el RUT no viene en la cartola ─
// Aplica a TS Capital (y cualquier empresa) donde el nombre puede aparecer sin RUT.
const NOMBRES_INTERNOS = [
  'TURRAGUZMAN',    // Hernán Ariel Turra Guzmán — normalizado sin espacios
];

function esNombreInterno(nombreOrigen) {
  if (!nombreOrigen) return false;
  const norm = String(nombreOrigen).toUpperCase().replace(/[\s.\-]/g, '');
  return NOMBRES_INTERNOS.some(frag => norm.includes(frag));
}

// Migración: marcar por nombre además de RUT
try {
  const byName = db.prepare(`
    UPDATE movimientos SET estado='interno', updated_at=datetime('now')
    WHERE estado IN ('pendiente','listo')
      AND UPPER(REPLACE(REPLACE(nombre_origen,' ',''),'.','')) LIKE '%TURRAGUZMAN%'
  `).run();
  if (byName.changes > 0)
    console.log(`[STARTUP] ${byName.changes} movimientos de Hernán Turra marcados como interno (por nombre)`);
} catch(e) { console.warn('[STARTUP] Error marcando Turra Guzmán por nombre:', e.message); }

// ── RUTs excluidos por empresa (representantes legales, socios propios, etc.) ──
// Vanher Capital: excluir RUT propio + representantes legales Vanessa Soto / Hernán Turra
try {
  db.exec(`UPDATE movimientos SET estado='interno', updated_at=datetime('now')
           WHERE empresa_id='vanher-capital'
           AND rut_normalizado IN ('779766063','168693591','167938582')
           AND estado IN ('pendiente','listo')`);
  console.log('[STARTUP] RUTs excluidos Vanher marcados como interno');
} catch(e) { console.warn('[STARTUP] Error marcando RUTs excluidos Vanher:', e.message); }

// ── Cleanup: eliminar intentos fallidos de facturación anteriores al deploy ──
// Resetea movimientos y borra lotes con estado 'error'. Seguro de correr múltiples veces.
try {
  db.transaction(() => {
    // Devolver movimientos de lotes fallidos a estado 'listo'
    db.exec(`UPDATE movimientos SET estado = 'listo', lote_id = NULL, updated_at = datetime('now')
             WHERE lote_id IN (SELECT lote_id FROM lotes_facturacion WHERE estado = 'error')`);
    // Eliminar los lotes con error
    db.exec(`DELETE FROM lotes_facturacion WHERE estado = 'error'`);
  })();
  console.log('[STARTUP] Limpieza de lotes con error completada');
} catch(e) { console.warn('[STARTUP] Error en limpieza de lotes:', e.message); }

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
          // Solo promover boletas (41) a 'listo'. Para facturas (34) NO bajar a 'pendiente':
          // si ya tenía datos completos y estaba 'listo', debe seguir disponible para facturar.
          const nuevoEstado = correcto === 41 ? 'listo' : m.estado;
          upd.run(correcto, nuevoEstado, now, m.id);
          fixed++;
        }
      }
    })();
    if (fixed > 0) console.log(`[MIGRATE] Re-clasificados ${fixed} movimientos con tipo_dte incorrecto`);
  } catch(e) { console.error('[MIGRATE] Error en reclassify:', e.message); }
})();

// ── Migration: restaurar movimientos tipo_34 que quedaron en 'pendiente' por bug ─
// El bug anterior bajaba de 'listo' a 'pendiente' al corregir tipo_dte.
// Si un movimiento tipo 34 tiene razon_social + email completos, puede facturarse.
try {
  const restored = db.prepare(`
    UPDATE movimientos
    SET estado = 'listo', updated_at = datetime('now')
    WHERE tipo_dte = 34
      AND estado = 'pendiente'
      AND razon_social IS NOT NULL AND razon_social != ''
      AND email_receptor IS NOT NULL AND email_receptor != ''
  `).run();
  if (restored.changes > 0)
    console.log(`[MIGRATION] Restaurados ${restored.changes} movimientos tipo_34 a estado='listo' (tenían datos completos)`);
} catch(e) { console.warn('[MIGRATION] Error restaurando movimientos tipo_34:', e.message); }

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

// ── Migration: asegurar que ts-capital tenga config Haulmer en la BD ──────────
(function ensureHaulmerConfig() {
  try {
    const empresas = getAppData('empresas');
    if (!empresas) return;
    const ts = empresas['ts-capital'];
    if (!ts) {
      // Si ts-capital no existe en BD pero sí en seed, insertarlo
      const seedPath = path.join(__dirname, 'seed-data.json');
      if (fs.existsSync(seedPath)) {
        const seed = JSON.parse(fs.readFileSync(seedPath, 'utf8'));
        if (seed.empresas?.['ts-capital']) {
          empresas['ts-capital'] = seed.empresas['ts-capital'];
          setAppData('empresas', empresas);
          console.log('[MIGRATE] ts-capital insertado desde seed-data.json');
        }
      }
    } else if (!ts.haulmer || !ts.haulmer.api_key || !ts.acteco) {
      // ts-capital existe pero le falta la config Haulmer — restaurar desde seed
      const seedPath = path.join(__dirname, 'seed-data.json');
      if (fs.existsSync(seedPath)) {
        const seed = JSON.parse(fs.readFileSync(seedPath, 'utf8'));
        const seedTs = seed.empresas?.['ts-capital'];
        if (seedTs?.haulmer) {
          ts.haulmer = { ...seedTs.haulmer, ...(ts.haulmer || {}) };
          ts.proveedor_dte = ts.proveedor_dte || seedTs.proveedor_dte || 'haulmer';
          ts.acteco = ts.acteco || seedTs.acteco || 643000;
          empresas['ts-capital'] = ts;
          setAppData('empresas', empresas);
          console.log('[MIGRATE] Restaurada config Haulmer + acteco para ts-capital');
        }
      }
    }
  } catch(e) { console.warn('[MIGRATE] ensureHaulmerConfig error:', e.message); }
})();

// ── Migration: asegurar que vanher-capital esté en la BD con config completa ──
(function ensureVanherCapital() {
  try {
    const empresas = getAppData('empresas');
    if (!empresas) return;
    const seedPath = path.join(__dirname, 'seed-data.json');
    if (!fs.existsSync(seedPath)) return;
    const seed = JSON.parse(fs.readFileSync(seedPath, 'utf8'));
    const seedVH = seed.empresas?.['vanher-capital'];
    if (!seedVH) return;

    if (!empresas['vanher-capital']) {
      // Primera vez: insertar completo
      empresas['vanher-capital'] = seedVH;
      setAppData('empresas', empresas);
      console.log('[MIGRATE] vanher-capital insertada desde seed-data.json');
    } else {
      // Ya existe: sincronizar campos que el admin no debería perder (ruts_excluidos, credenciales SF)
      const vh = empresas['vanher-capital'];
      let changed = false;

      if (!vh.ruts_excluidos || vh.ruts_excluidos.length < 3) {
        vh.ruts_excluidos = seedVH.ruts_excluidos;
        changed = true;
      }
      // Restaurar credenciales SF si faltan
      if (!vh.simplefactura) vh.simplefactura = {};
      const sf = vh.simplefactura;
      const sfSeed = seedVH.simplefactura || {};
      if (!sf.api_token   && sfSeed.api_token)   { sf.api_token = sfSeed.api_token; changed = true; }
      if (!sf.username    && sfSeed.username)    { sf.username  = sfSeed.username;  changed = true; }
      if (!sf.password    && sfSeed.password)    { sf.password  = sfSeed.password;  changed = true; }
      if (!sf.rut_emisor  && sfSeed.rut_emisor)  { sf.rut_emisor = sfSeed.rut_emisor; changed = true; }
      if (!sf.rut_emisor_sf && sfSeed.rut_emisor_sf) { sf.rut_emisor_sf = sfSeed.rut_emisor_sf; changed = true; }
      if (!sf.nombre_sucursal) { sf.nombre_sucursal = sfSeed.nombre_sucursal || 'Casa Matriz'; changed = true; }

      if (changed) {
        vh.simplefactura = sf;
        empresas['vanher-capital'] = vh;
        setAppData('empresas', empresas);
        console.log('[MIGRATE] Credenciales y config Vanher actualizadas en BD');
      }
    }
  } catch(e) { console.warn('[MIGRATE] ensureVanherCapital error:', e.message); }
})();

// ── Password migration: actualizar credenciales y renombrar hturra→admin ─────
(function migratePasswords() {
  try {
    const users = getAppData('users');
    if (!users) return;
    let changed = false;
    // Revertir admin → hturra si se renombró por error
    if (users.admin && !users.hturra) {
      users.hturra = { ...users.admin };
      delete users.admin;
      changed = true;
      console.log('[MIGRATE] Revertido usuario admin → hturra');
    }
    // Actualizar hashes de contraseñas
    const newHashes = {
      hturra:    '1a36e3204acafe38cf3ef45f0bfdae04d527e1ab2f503d574ad33f0c7d3243dc',
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
  // Aceptar token desde header Authorization o desde query param ?token=
  // El query param es necesario para window.open() (descargas CSV en nueva pestaña)
  const h = req.headers.authorization;
  const raw = h?.startsWith('Bearer ') ? h.slice(7) : (req.query.token || null);
  if (!raw) return res.status(401).json({ error: 'No token' });
  try {
    req.user = jwt.verify(raw, JWT_SECRET);
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

// Empresas cuya facturación está restringida a rol admin
const EMPRESAS_SOLO_ADMIN = ['vanher-capital'];

// Devuelve true y responde 403 si el usuario no tiene permiso para facturar esa empresa
function checkEmpresaAdminOnly(req, res, empresaId) {
  if (EMPRESAS_SOLO_ADMIN.includes(empresaId) && req.user.role !== 'admin') {
    res.status(403).json({ error: 'Solo el administrador puede facturar esta empresa' });
    return true;
  }
  return false;
}

// Devuelve true si el RUT está excluido de facturación para una empresa específica
// Combina los globales (RUTS_INTERNOS) con los excluidos por config de empresa
function esRutExcluido(rutNorm, empresaId) {
  if (rutNorm && RUTS_INTERNOS.includes(rutNorm)) return true;
  const empConf = getAppData('empresas')?.[empresaId] || {};
  const excluidos = empConf.ruts_excluidos || [];
  if (rutNorm && excluidos.includes(rutNorm)) return true;
  return false;
}

// Devuelve true si el movimiento debe excluirse por nombre (cuando RUT no disponible)
function esMovimientoInterno(rutNorm, nombreOrigen, empresaId) {
  if (esRutExcluido(rutNorm, empresaId)) return true;
  if (esNombreInterno(nombreOrigen)) return true;
  return false;
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

// Devuelve la fecha más reciente de un array de movimientos, formateada DD-MM-YYYY
function maxFechaLote(movs) {
  let max = null;
  for (const m of movs) {
    if (!m.fecha) continue;
    const f = String(m.fecha).trim();
    let d, match;
    if ((match = f.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/)))
      d = new Date(`${match[3]}-${match[2].padStart(2,'0')}-${match[1].padStart(2,'0')}`);
    else if ((match = f.match(/^(\d{4})-(\d{2})-(\d{2})/)))
      d = new Date(`${match[1]}-${match[2]}-${match[3]}`);
    else if ((match = f.match(/^(\d{2})-(\d{2})-(\d{4})/)))
      d = new Date(`${match[3]}-${match[2]}-${match[1]}`);
    if (d && !isNaN(d.getTime()) && (!max || d > max)) max = d;
  }
  if (!max) return '';
  const dd = String(max.getDate()).padStart(2, '0');
  const mm = String(max.getMonth() + 1).padStart(2, '0');
  return `${dd}-${mm}-${max.getFullYear()}`;
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
  const rutNorm = normalizeRut(c.rut);

  db.transaction(() => {
    // Actualizar datos del cliente
    db.prepare(`
      UPDATE clientes SET tipo=?, rut=?, rut_normalizado=?, razon_social=?, giro=?, direccion=?, comuna=?, ciudad=?, nombre=?, email=?, telefono=?, representante_legal=?, rut_representante=?, updated_at=?
      WHERE id=?
    `).run(c.tipo, c.rut, rutNorm, c.razon_social, c.giro, c.direccion, c.comuna, c.ciudad, c.nombre, c.email, c.telefono, c.representante_legal, c.rut_representante, now, req.params.id);

    // Sincronizar email_receptor en movimientos pendientes/listos del mismo RUT
    if (c.email && rutNorm) {
      const synced = db.prepare(`
        UPDATE movimientos
        SET email_receptor = ?, updated_at = ?
        WHERE rut_normalizado = ? AND estado IN ('pendiente', 'listo') AND (email_receptor IS NULL OR email_receptor = '')
      `).run(c.email, now, rutNorm);
      console.log(`[CLIENTES] Email sincronizado en ${synced.changes} movimientos para RUT ${rutNorm}`);
    }
  })();

  res.json({ ok: true });
});

app.delete('/api/clientes/:id', requireAuth, requireAdmin, (req, res) => {
  db.prepare('DELETE FROM clientes WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

// ── Email rápido: asignar email a un cliente por RUT (y sincronizar movimientos) ──
// Uso desde Binance: el operador copia el email del chat y lo asigna al RUT del cliente
app.post('/api/clientes/email-rapido', requireAuth, (req, res) => {
  const { rut, email } = req.body;
  if (!rut || !email) return res.status(400).json({ error: 'RUT y email son requeridos' });
  const rutNorm = normalizeRut(rut);
  const empresaId = filterByEmpresa(req);
  const now = nowCL();

  let clienteId = null;
  let accion = 'no_encontrado';
  let movsSincronizados = 0;

  db.transaction(() => {
    // Buscar cliente existente
    const cliente = empresaId
      ? db.prepare('SELECT * FROM clientes WHERE rut_normalizado = ? AND empresa_id = ?').get(rutNorm, empresaId)
      : db.prepare('SELECT * FROM clientes WHERE rut_normalizado = ?').get(rutNorm);

    if (cliente) {
      // Actualizar email del cliente existente
      db.prepare('UPDATE clientes SET email = ?, updated_at = ? WHERE id = ?').run(email, now, cliente.id);
      clienteId = cliente.id;
      accion = 'actualizado';
    } else {
      // Crear cliente mínimo si no existe (se completará luego)
      const result = db.prepare(`
        INSERT INTO clientes (empresa_id, tipo, rut, rut_normalizado, email, created_at, updated_at)
        VALUES (?, 'empresa', ?, ?, ?, ?, ?)
      `).run(empresaId || 'mt', rut, rutNorm, email, now, now);
      clienteId = result.lastInsertRowid;
      accion = 'creado';
    }

    // Sincronizar email en todos los movimientos de ese RUT que estén pendientes/listos
    const empQuery = empresaId ? ' AND empresa_id = ?' : '';
    const empParams = empresaId ? [email, now, rutNorm, empresaId] : [email, now, rutNorm];
    const synced = db.prepare(`
      UPDATE movimientos
      SET email_receptor = ?, updated_at = ?
      WHERE rut_normalizado = ? AND estado IN ('pendiente', 'listo') ${empQuery}
    `).run(...empParams);
    movsSincronizados = synced.changes;
  })();

  console.log(`[EMAIL-RAPIDO] RUT ${rutNorm} → ${email} (${accion}) | ${movsSincronizados} movs sincronizados`);
  res.json({ ok: true, accion, clienteId, movsSincronizados });
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
  const { estado, fecha_desde, fecha_hasta, lote_id, tipo_dte, banco, limit: lim, offset: off, pag, orden, dir } = req.query;
  let sql = 'SELECT * FROM movimientos WHERE 1=1';
  const params = [];

  if (empresaId) { sql += ' AND empresa_id = ?'; params.push(empresaId); }
  if (estado) { sql += ' AND estado = ?'; params.push(estado); }
  if (tipo_dte) { sql += ' AND tipo_dte = ?'; params.push(parseInt(tipo_dte)); }
  if (banco) { sql += ' AND banco_cartola = ?'; params.push(banco); }
  if (fecha_desde) { sql += ' AND fecha >= ?'; params.push(fecha_desde); }
  if (fecha_hasta) { sql += ' AND fecha <= ?'; params.push(fecha_hasta); }
  if (lote_id) { sql += ' AND lote_id = ?'; params.push(lote_id); }

  // Columnas permitidas para ORDER BY (whitelist anti-injection)
  const COLS_FACT = { fecha: 'fecha', monto: 'monto', rut: 'rut', razon_social: 'razon_social', id: 'id' };
  const sortCol = COLS_FACT[orden] || 'fecha';
  const sortDir = dir === 'desc' ? 'DESC' : 'ASC';

  // Modo paginado: devuelve { movimientos, total, total_34, total_41 }
  if (pag === '1') {
    const countSql = sql.replace('SELECT * FROM movimientos WHERE 1=1', 'SELECT COUNT(*) as total FROM movimientos WHERE 1=1');
    const total = db.prepare(countSql).get(...params)?.total || 0;
    // Conteo por tipo DTE PENDIENTES (estado=listo, SIN filtro tipo_dte para mostrar ambos totales siempre)
    let typeSql = 'SELECT tipo_dte, COUNT(*) as cnt FROM movimientos WHERE 1=1';
    const typeParams = [];
    if (empresaId) { typeSql += ' AND empresa_id = ?'; typeParams.push(empresaId); }
    if (estado)    { typeSql += ' AND estado = ?';     typeParams.push(estado); }
    if (banco)     { typeSql += ' AND banco_cartola = ?'; typeParams.push(banco); }
    if (fecha_desde) { typeSql += ' AND fecha >= ?';   typeParams.push(fecha_desde); }
    if (fecha_hasta) { typeSql += ' AND fecha <= ?';   typeParams.push(fecha_hasta); }
    typeSql += ' GROUP BY tipo_dte';
    const dteCounts = db.prepare(typeSql).all(...typeParams);
    const total_34 = dteCounts.find(r => r.tipo_dte === 34)?.cnt || 0;
    const total_41 = dteCounts.find(r => r.tipo_dte === 41)?.cnt || 0;
    sql += ` ORDER BY ${sortCol} ${sortDir}, id ${sortDir}`;
    if (lim) { sql += ' LIMIT ?'; params.push(parseInt(lim)); }
    if (off) { sql += ' OFFSET ?'; params.push(parseInt(off)); }
    return res.json({ movimientos: db.prepare(sql).all(...params), total, total_34, total_41 });
  }

  // Modo legado: devuelve array directo (usado por exportar, reclasificar, etc.)
  sql += ' ORDER BY id DESC';
  if (lim) { sql += ' LIMIT ?'; params.push(parseInt(lim)); }
  if (off) { sql += ' OFFSET ?'; params.push(parseInt(off)); }
  res.json(db.prepare(sql).all(...params));
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

  const cargadoPor = req.user.username || 'sistema';
  const checkDup = db.prepare('SELECT id, estado FROM movimientos WHERE id_compuesto = ? AND empresa_id = ?');
  const insertMov = db.prepare(`
    INSERT INTO movimientos (empresa_id, id_transferencia, fecha, monto, glosa, rut, rut_normalizado, nombre_origen, banco_origen, banco_cartola, cuenta_origen, id_compuesto, estado, tipo_dte, razon_social, giro, direccion, comuna, ciudad, email_receptor, nombre_item, descripcion_item, precio, monto_exento, monto_total, fecha_carga, created_at, updated_at, lote_carga_id, cargado_por)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        const idCompuesto = `${empresaId}_${idTransf}_${banco_cartola}`;

        // Check duplicate
        const existing = checkDup.get(idCompuesto, empresaId);
        if (existing) {
          duplicados++;
          results.push({ id_compuesto: idCompuesto, status: 'duplicado', existing_estado: existing.estado });
          continue;
        }

        const rutNorm = normalizeRut(mov.rut || '');
        let estado = 'pendiente';
        let tipoDte = null;
        let razonSocial = '', giro = '', direccion = '', comuna = '', ciudad = '', emailReceptor = '';

        // RUT o nombre excluido: empresas propias, representantes o personas internas
        if (esMovimientoInterno(rutNorm, mov.nombre_origen, empresaId)) {
          estado = 'interno';
        } else if (rutNorm) {
          // Determine DTE type by RUT using empresa config (tipo_dte_personas / tipo_dte_empresas)
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
          monto, tipoDte ? monto : 0, monto,
          now, now, now, loteCargaId, cargadoPor
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
  if (estado === 'facturado') {
    // Al marcar facturado, usar la fecha de la transferencia como fecha_facturacion
    const stmt = db.prepare('UPDATE movimientos SET estado = ?, fecha_facturacion = COALESCE(fecha, ?), updated_at = ? WHERE id = ?');
    db.transaction(() => {
      for (const id of ids) stmt.run(estado, now, now, id);
    })();
  } else {
    const stmt = db.prepare('UPDATE movimientos SET estado = ?, updated_at = ? WHERE id = ?');
    db.transaction(() => {
      for (const id of ids) stmt.run(estado, now, id);
    })();
  }
  res.json({ ok: true, updated: ids.length });
});

// ── Historial de cargas de cartola ───────────────────────────────────────────
app.get('/api/cartolas/historial', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  let sql = `SELECT lote_carga_id, empresa_id, banco_cartola, fecha_carga,
    MAX(cargado_por) as cargado_por,
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
      // Solo promover boletas (41) a 'listo'. Para facturas (34) conservar estado actual:
      // bajar de 'listo' a 'pendiente' ocultaría documentos ya listos para emitir.
      const nuevoEstado = correcto === 41 ? 'listo' : m.estado;
      db.prepare('UPDATE movimientos SET tipo_dte=?, estado=?, updated_at=? WHERE id=?')
        .run(correcto, nuevoEstado, now, m.id);
      fixed++;
    }
  })();
  res.json({ ok: true, reclasificados: fixed });
});

// ── Facturación (crear lote + enviar a SimpleFactura) ────────────────────────
app.post('/api/facturacion/crear-lote', requireAuth, (req, res) => {
  const { empresa_id, movimiento_ids, tipo_dte } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  if (checkEmpresaAdminOnly(req, res, empresaId)) return;
  const now = nowCL();
  const loteId = generateLoteId(empresaId);

  // Get movimientos — dos modos:
  // 1. Por IDs específicos (selección manual o por página)
  // 2. Por tipo_dte (todos los 'listo' de ese tipo en la empresa)
  let movs;
  if (tipo_dte && !movimiento_ids?.length) {
    // Modo tipo: traer TODOS los movimientos listo del tipo dado para esta empresa
    if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });
    movs = db.prepare(
      `SELECT * FROM movimientos WHERE empresa_id = ? AND estado = 'listo' AND tipo_dte = ? ORDER BY fecha ASC`
    ).all(empresaId, parseInt(tipo_dte));
  } else {
    const placeholders = (movimiento_ids || []).map(() => '?').join(',');
    if (!placeholders) return res.status(400).json({ error: 'Sin movimientos ni tipo_dte' });
    movs = db.prepare(`SELECT * FROM movimientos WHERE id IN (${placeholders}) AND estado = 'listo'`).all(...movimiento_ids);
  }

  if (movs.length === 0) return res.status(400).json({ error: 'No hay movimientos listos para facturar' });

  const montoTotal = movs.reduce((s, m) => s + (m.monto_total || 0), 0);
  const ultimaFecha = maxFechaLote(movs);
  const nombre = `Lote ${ultimaFecha} ${movs.length} DTE`.trim();

  db.transaction(() => {
    // Create lote con nombre descriptivo
    db.prepare('INSERT INTO lotes_facturacion (lote_id, empresa_id, nombre, cantidad, monto_total, estado, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)')
      .run(loteId, empresaId, nombre, movs.length, montoTotal, 'pendiente', now, now);
    // Update movimientos
    const updStmt = db.prepare("UPDATE movimientos SET lote_id = ?, estado = 'en_lote', updated_at = ? WHERE id = ?");
    for (const m of movs) updStmt.run(loteId, now, m.id);
  })();

  res.json({ ok: true, lote_id: loteId, nombre, cantidad: movs.length, monto_total: montoTotal });
});

// Marcar movimientos como facturados manualmente (sin emitir vía API)
// Crea un lote registral "Marcado facturado manual" y pasa los movimientos a estado 'facturado'
app.post('/api/facturacion/marcar-manual', requireAuth, (req, res) => {
  const { empresa_id, movimiento_ids } = req.body;
  if (!Array.isArray(movimiento_ids) || !movimiento_ids.length)
    return res.status(400).json({ error: 'Sin movimientos' });
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });

  const now = nowCL();
  const loteId = generateLoteId(empresaId);
  const nombre = 'Marcado facturado manual';

  const placeholders = movimiento_ids.map(() => '?').join(',');
  const movs = db.prepare(
    `SELECT * FROM movimientos WHERE id IN (${placeholders}) AND estado = 'listo'`
  ).all(...movimiento_ids);

  if (!movs.length) return res.status(400).json({ error: 'No hay movimientos listos para marcar' });

  const montoTotal = movs.reduce((s, m) => s + (m.monto_total || 0), 0);

  db.transaction(() => {
    db.prepare(
      'INSERT INTO lotes_facturacion (lote_id, empresa_id, nombre, cantidad, monto_total, estado, metodo, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)'
    ).run(loteId, empresaId, nombre, movs.length, montoTotal, 'facturado_manual', 'manual', now, now);

    const updStmt = db.prepare(
      "UPDATE movimientos SET lote_id = ?, estado = 'facturado', fecha_facturacion = ?, updated_at = ? WHERE id = ?"
    );
    for (const m of movs) updStmt.run(loteId, m.fecha || now, now, m.id);
  })();

  res.json({ ok: true, lote_id: loteId, nombre, cantidad: movs.length });
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

// Eliminar lote (solo admin) — devuelve movimientos a estado 'listo'
app.delete('/api/facturacion/lotes/:lote_id', requireAuth, (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo administradores pueden eliminar lotes' });
  const loteId = req.params.lote_id;
  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });
  if (lote.estado === 'emitido') return res.status(400).json({ error: 'No se puede eliminar un lote ya emitido' });
  const now = nowCL();
  db.transaction(() => {
    db.prepare(`UPDATE movimientos SET estado = 'listo', lote_id = NULL, updated_at = ? WHERE lote_id = ?`).run(now, loteId);
    db.prepare(`DELETE FROM lotes_facturacion WHERE lote_id = ?`).run(loteId);
  })();
  console.log(`[LOTE DELETE] Lote ${loteId} eliminado por ${req.user.username}`);
  res.json({ ok: true, mensaje: `Lote eliminado. Los movimientos volvieron a estado "listo".` });
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

// Quitar un movimiento específico del lote (admin) — lo devuelve a estado 'listo'
// Útil cuando un documento tiene error OF-10 u otro y se quiere procesar por separado.
app.post('/api/facturacion/lotes/:lote_id/quitar-movimiento', requireAuth, (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo administradores' });
  const loteId   = req.params.lote_id;
  const movId    = parseInt(req.body.movimiento_id);
  if (!movId) return res.status(400).json({ error: 'movimiento_id requerido' });

  const lote = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });
  if (lote.estado === 'emitido') return res.status(400).json({ error: 'No se puede modificar un lote emitido' });

  const mov = db.prepare("SELECT * FROM movimientos WHERE id = ? AND lote_id = ?").get(movId, loteId);
  if (!mov) return res.status(404).json({ error: 'Movimiento no encontrado en este lote' });
  if (mov.estado === 'facturado') return res.status(400).json({ error: 'El movimiento ya está facturado' });

  const now = nowCL();
  db.transaction(() => {
    // Devolver movimiento a estado listo, desligarlo del lote
    db.prepare("UPDATE movimientos SET estado='listo', lote_id=NULL, updated_at=? WHERE id=?").run(now, movId);
    // Recalcular totales del lote
    const nuevaCantidad = Math.max(0, (lote.cantidad || 1) - 1);
    const nuevoMonto    = Math.max(0, (lote.monto_total || 0) - (mov.monto_total || mov.monto || 0));
    db.prepare("UPDATE lotes_facturacion SET cantidad=?, monto_total=?, updated_at=? WHERE lote_id=?")
      .run(nuevaCantidad, nuevoMonto, now, loteId);
  })();
  console.log(`[LOTE] Movimiento ${movId} quitado de lote ${loteId} por ${req.user.username}`);
  res.json({ ok: true, mensaje: 'Movimiento quitado del lote. Vuelve a "listo" en Facturación.' });
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

// Obtener token SF: prioridad → api_token directo → login email/password
// Si api_token está configurado se usa como Bearer directamente (APIs con clave estática).
// Si no, se hace login con email+password para obtener JWT temporal.
async function sfGetToken(email, password, apiToken) {
  // 0. Si hay API token estático, usarlo directamente como Bearer
  if (apiToken && apiToken.length > 10) {
    console.log(`[SF TOKEN] Usando API token estático para ${email}`);
    return apiToken;
  }

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
  // Eliminar CR/LF que rompen filas CSV (e.g. emails con newline al final)
  const s = String(val ?? '').replace(/[\r\n\t]/g, ' ').trim();
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

// ══════════════════════════════════════════════════════════════════════════════
// MÓDULO: OPEN FACTURA (HAULMER) — TS CAPITAL
// API Key: configurada en empresa.haulmer.api_key
// Docs: https://docs.haulmer.com/
// ══════════════════════════════════════════════════════════════════════════════

// Haulmer CSV format (semicolon-separated, 2-row header, no BOM needed)
// Formato exacto obtenido de archivos CSV de TS Capital: Boletas 1 - Febrero 2026 TS.csv
const HAULMER_CSV_ROW1  = 'generales;;;;;receptor;;;;;detalles;;;totales;;;';
const HAULMER_CSV_ROW2  = [
  'Tipo de documento (*)',
  'Fecha de emisión (*)',
  'Tipo de venta (*)',
  'Forma de pago (*)',
  'Tipo de servicio (*)',
  'RUT Receptor (*)',
  'Razón Social (*)',
  'Giro (*)',
  'Dirección (*)',
  'Comuna (*)',
  'Nombre de Item (*)',
  'Cantidad (*)',
  'Precio (*)',
  'Monto Neto (*)',
  'Monto Exento (*)',
  'Monto IVA (*)',
  'Monto Total (*)'
].join(';');

function escaparCsvHaulmer(val) {
  // Eliminar CR/LF que rompen filas CSV
  const s = String(val ?? '').replace(/[\r\n\t]/g, ' ').trim();
  // Envolver en comillas si contiene punto y coma o comillas
  if (s.includes(';') || s.includes('"')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

// Formato fecha DD-MM-YYYY para exportación Excel
// Acepta: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, DD/MM/YYYY, DD-MM-YYYY, DD/MM/YYYY HH:MM
function formatoFechaDDMMYYYY(fecha) {
  if (!fecha) return '';
  const s = String(fecha).trim().split('T')[0].split(' ')[0]; // quitar hora si hay
  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const [y, m, d] = s.split('-');
    return `${d}-${m}-${y}`;
  }
  // DD/MM/YYYY
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [d, m, y] = s.split('/');
    return `${d.padStart(2,'0')}-${m.padStart(2,'0')}-${y}`;
  }
  // DD-MM-YYYY (ya está en el formato correcto)
  if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(s)) {
    const [d, m, y] = s.split('-');
    return `${d.padStart(2,'0')}-${m.padStart(2,'0')}-${y}`;
  }
  return s; // fallback
}

// Formato fecha para Haulmer: YYYY-MM-DD
function fechaParaHaulmer(fechaISO) {
  if (!fechaISO) return new Date().toISOString().slice(0,10);
  // Si viene DD/MM/YYYY
  if (/^\d{1,2}\/\d{1,2}\/\d{4}/.test(String(fechaISO))) {
    const [d,m,y] = String(fechaISO).split('/');
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  return String(fechaISO).slice(0,10);
}

function buildHaulmerCsvContent(movs, empresa) {
  const email = empresa.email_facturacion || 'facturas@tscapitalchile.cl';
  const nombreItem = empresa.nombre_item_default || 'Venta Activo Digital. Transferencia a Banco';
  let csv = HAULMER_CSV_ROW1 + '\n' + HAULMER_CSV_ROW2 + '\n';
  for (const m of movs) {
    const tipoDte = getTipoDte(m.rut_normalizado, empresa);
    const fecha   = fechaParaHaulmer(m.fecha);
    const rut     = m.rut ? m.rut : '';   // con puntos y guión
    const razon   = (m.razon_social || m.nombre_origen || '').substring(0, 100);
    const giro    = (m.giro || 'NO INFORMADA').substring(0, 80);
    const dir     = (m.direccion || 'NO INFORMADA').substring(0, 100);
    const comuna  = m.comuna || 'NO INFORMADA';
    const monto   = Math.round(m.monto_total || m.monto || 0);
    const itemNombre = (m.nombre_item || `${nombreItem} ${m.banco_cartola || ''}`).trim();

    const row = [
      tipoDte,                    // 34 o 41
      fecha,                      // YYYY-MM-DD
      ' Ventas del Giro',
      ' Contado',
      '',                         // Tipo de servicio (vacío para TS Capital)
      rut,
      razon,
      giro,
      dir,
      comuna,
      itemNombre,                 // Nombre de Item
      1,                          // Cantidad
      monto,                      // Precio
      0,                          // Monto Neto
      monto,                      // Monto Exento
      0,                          // Monto IVA
      monto                       // Monto Total
    ];
    csv += row.map(escaparCsvHaulmer).join(';') + '\n';
  }
  // BOM UTF-8 para compatibilidad Excel
  return '\uFEFF' + csv;
}

// ── Exportar CSV formato Haulmer para un lote ─────────────────────────────────
app.get('/api/facturacion/exportar-haulmer-csv/:lote_id', requireAuth, (req, res) => {
  const loteId = req.params.lote_id;
  const movs = db.prepare('SELECT * FROM movimientos WHERE lote_id = ?').all(loteId);
  if (!movs.length) return res.status(404).json({ error: 'Lote sin movimientos' });

  const empresas = getAppData('empresas');
  const lote     = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  const empresa  = empresas[lote?.empresa_id] || {};

  const csv = buildHaulmerCsvContent(movs, empresa);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Haulmer_${lote?.empresa_id}_${loteId}.csv"`);
  res.send(csv);
});

// ── Exportar CSV Haulmer para selección manual (sin lote) ─────────────────────
app.post('/api/facturacion/exportar-haulmer-seleccion', requireAuth, (req, res) => {
  const { movimiento_ids, empresa_id } = req.body;
  const empresaId = req.user.role === 'admin' ? empresa_id : req.user.empresa;
  if (!movimiento_ids?.length) return res.status(400).json({ error: 'Sin movimientos seleccionados' });

  const placeholders = movimiento_ids.map(() => '?').join(',');
  const movs = db.prepare(`SELECT * FROM movimientos WHERE id IN (${placeholders})`).all(...movimiento_ids);
  const empresa = getAppData('empresas')?.[empresaId] || {};

  const csv = buildHaulmerCsvContent(movs, empresa);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="Haulmer_${empresaId}_seleccion.csv"`);
  res.send(csv);
});

// ── Emitir DTE vía Open Factura (Haulmer) API ────────────────────────────────
// Haulmer acepta JSON individual o CSV masivo. Usamos JSON masivo (array de DTEs).
// Endpoint: POST https://api.haulmer.com/v2/dte/document
// Auth header: apikey: <api_key>
app.post('/api/facturacion/emitir-haulmer/:lote_id', requireAuth, async (req, res) => {
  const loteId  = req.params.lote_id;
  const lote    = db.prepare('SELECT * FROM lotes_facturacion WHERE lote_id = ?').get(loteId);
  if (!lote) return res.status(404).json({ error: 'Lote no encontrado' });
  if (checkEmpresaAdminOnly(req, res, lote.empresa_id)) return;

  const empresas = getAppData('empresas');
  const empresa  = empresas[lote.empresa_id] || {};
  const hConf    = empresa.haulmer || {};
  const apiKey   = hConf.api_key || '';
  if (!apiKey) return res.status(400).json({ error: 'API Key de Open Factura (Haulmer) no configurada' });

  // Resetear movimientos en error antes de reintentar
  const now = nowCL();
  db.prepare("UPDATE movimientos SET estado='en_lote', updated_at=? WHERE lote_id=? AND estado IN ('error','listo')").run(now, loteId);
  const movs = db.prepare("SELECT * FROM movimientos WHERE lote_id=? AND estado='en_lote'").all(loteId);
  if (!movs.length) return res.status(400).json({ error: 'No hay movimientos en este lote' });

  // Construir payload JSON para Haulmer API v2
  // Formato: { response: [...], dte: { Encabezado: {...}, Detalle: [...] } }
  // Docs: docsapi-openfactura.haulmer.com
  const rutEmisor  = (empresa.rut || '77.506.343-2').replace(/\./g, ''); // sin puntos, con guión
  const giroEmisor = empresa.giro || 'FONDOS Y SOCIEDADES DE INVERSION Y ENTIDADES FINANCIERAS SIMILARES';
  const emailFact  = empresa.email_facturacion || 'facturas@tscapitalchile.cl';
  const nombreItem = empresa.nombre_item_default || 'Venta Activo Digital';
  const dirOrigen  = empresa.direccion || 'MANQUEHUE NORTE 151 OF 1205 PS 12';
  const cmnaOrigen = empresa.comuna || 'Las Condes';
  const ciudadOrigen = empresa.ciudad || 'SANTIAGO';
  const rznSocEmisor = empresa.nombre || 'TS Capital SPA';

  const HAULMER_API_URL = 'https://api.haulmer.com/v2/dte/document';

  let emitidos = 0, erroresEmision = 0;
  const resultados = [];

  try {
    for (const m of movs) {
      const tipoDte = getTipoDte(m.rut_normalizado, empresa);
      const fecha   = fechaParaHaulmer(m.fecha);
      const monto   = Math.round(m.monto_total || m.monto || 0);

      // Payload Haulmer v2 — orden de campos sigue esquema SII estricto
      // Boletas (39/41): DTE_v10 + EnvioBOLETA_v11.xsd → NO tiene FmaPago
      // Facturas (33/34): DTE_v10.xsd → tiene FmaPago
      const esBoleta = (tipoDte === 39 || tipoDte === 41);
      const idDoc = { TipoDTE: tipoDte, FchEmis: fecha };
      if (esBoleta) {
        idDoc.IndServicio = 3;   // 3 = Ventas y Servicios
      } else {
        idDoc.IndServicio = 3;
        idDoc.FmaPago = 1;       // 1 = Contado (solo facturas)
      }

      // Emisor y Receptor tienen campos diferentes para Boletas vs Facturas
      // Boletas (EnvioBOLETA_v11.xsd): RznSocEmisor, GiroEmisor, CdgSIISucur, DirOrigen...
      // Facturas (DTE_v10.xsd): RznSoc, GiroEmis, CorreoEmisor, Acteco, DirOrigen...
      const emisor = esBoleta ? {
        RUTEmisor:    rutEmisor,
        RznSocEmisor: rznSocEmisor,
        GiroEmisor:   giroEmisor,
        DirOrigen:    dirOrigen,
        CmnaOrigen:   cmnaOrigen,
        CiudadOrigen: ciudadOrigen
      } : {
        RUTEmisor:    rutEmisor,
        RznSoc:       rznSocEmisor,
        GiroEmis:     giroEmisor,
        CorreoEmisor: emailFact,
        Acteco:       empresa.acteco || 643000,
        DirOrigen:    dirOrigen,
        CmnaOrigen:   cmnaOrigen,
        CiudadOrigen: ciudadOrigen
      };

      // Receptor Boleta: RUTRecep, RznSocRecep, DirRecep, CmnaRecep, CiudadRecep
      // Receptor Factura: RUTRecep, RznSocRecep, GiroRecep, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep
      const receptor = esBoleta ? {
        RUTRecep:    (m.rut || '').replace(/\./g, ''),
        RznSocRecep: (m.razon_social || m.nombre_origen || '').substring(0, 100),
        DirRecep:    (m.direccion || 'NO INFORMADA').substring(0, 100),
        CmnaRecep:   m.comuna || 'NO INFORMADA',
        CiudadRecep: m.ciudad || ciudadOrigen
      } : {
        RUTRecep:    (m.rut || '').replace(/\./g, ''),
        RznSocRecep: (m.razon_social || m.nombre_origen || '').substring(0, 100),
        GiroRecep:   (m.giro || 'NO INFORMADA').substring(0, 80),
        CorreoRecep: m.email_receptor || emailFact,
        DirRecep:    (m.direccion || 'NO INFORMADA').substring(0, 100),
        CmnaRecep:   m.comuna || 'NO INFORMADA',
        CiudadRecep: m.ciudad || ciudadOrigen
      };

      const payload = {
        response: ['FOLIO'],
        dte: {
          Encabezado: {
            IdDoc: idDoc,
            Emisor: emisor,
            Receptor: receptor,
            Totales: {
              MntExe:   monto,
              MntTotal: monto
            }
          },
          Detalle: [{
            NroLinDet: 1,
            NmbItem:   (m.nombre_item || nombreItem + ' ' + (m.banco_cartola || '')).trim(),
            QtyItem:   1,
            PrcItem:   monto,
            MontoItem: monto,
            IndExe:    1   // exento
          }]
        }
      };

      // Idempotency-Key para evitar duplicados (basada en mov ID + lote)
      const idempKey = `${loteId}-mov-${m.id}-${Date.now()}`;

      try {
        const resp = await fetch(HAULMER_API_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'apikey': apiKey,
            'Idempotency-Key': idempKey
          },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(15000)
        });
        const raw = await resp.text();
        let data;
        try { data = JSON.parse(raw); } catch(e) { data = { raw }; }
        console.log(`[HAULMER] DTE ${tipoDte} mov ${m.id} → HTTP ${resp.status}: ${raw.substring(0,200)}`);

        if (resp.ok && !data?.error) {
          db.prepare("UPDATE movimientos SET estado='facturado', fecha_facturacion=?, updated_at=? WHERE id=?")
            .run(now, now, m.id);
          emitidos++;
          resultados.push({ id: m.id, status: 'emitido', folio: data?.folio || data?.Folio, resp: raw.substring(0,200) });
        } else {
          db.prepare("UPDATE movimientos SET estado='error', updated_at=? WHERE id=?").run(now, m.id);
          erroresEmision++;
          resultados.push({ id: m.id, status: 'error', httpStatus: resp.status, resp: raw.substring(0,200) });
        }
      } catch(dteErr) {
        console.error(`[HAULMER] Error mov ${m.id}:`, dteErr.message);
        db.prepare("UPDATE movimientos SET estado='error', updated_at=? WHERE id=?").run(now, m.id);
        erroresEmision++;
        resultados.push({ id: m.id, status: 'error', error: dteErr.message });
      }
    }

    const loteEstado = emitidos === movs.length ? 'emitido' : (emitidos > 0 ? 'parcial' : 'error');
    const mensaje = `Haulmer: ${emitidos} emitidos, ${erroresEmision} errores de ${movs.length} DTEs`;
    db.prepare("UPDATE lotes_facturacion SET estado=?, response_api=?, updated_at=? WHERE lote_id=?")
      .run(loteEstado, JSON.stringify({ mensaje, emitidos, erroresEmision, resultados: resultados.slice(0,50) }), now, loteId);

    res.json({ ok: emitidos > 0, emitidos, errores: erroresEmision, total: movs.length, mensaje, resultados: resultados.slice(0,50) });
  } catch (err) {
    console.error('[HAULMER FATAL]', err);
    db.prepare("UPDATE lotes_facturacion SET estado='error', response_api=?, updated_at=? WHERE lote_id=?")
      .run(JSON.stringify({ error: err.message }), now, loteId);
    res.status(500).json({ error: 'Error al emitir con Open Factura: ' + err.message });
  }
});

// ── Test conexión Haulmer ─────────────────────────────────────────────────────
app.get('/api/facturacion/test-haulmer/:empresa_id', requireAuth, async (req, res) => {
  const empresas = getAppData('empresas');
  const empresa = empresas[req.params.empresa_id];
  const hConf = empresa?.haulmer || {};
  if (!hConf.api_key) return res.status(400).json({ error: 'API Key de Haulmer no configurada' });

  try {
    // Haulmer no tiene un endpoint de "ping" oficial, pero un GET al endpoint de documentos
    // con la API key devuelve un error controlado si la key es válida vs 401 si no lo es
    const resp = await fetch('https://api.haulmer.com/v2/dte/document', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': hConf.api_key
      },
      body: JSON.stringify({}), // payload vacío — esperamos error de validación, no 401
      signal: AbortSignal.timeout(10000)
    });
    const raw = await resp.text();
    console.log(`[HAULMER TEST] HTTP ${resp.status}: ${raw.substring(0,300)}`);

    if (resp.status === 401 || resp.status === 403) {
      return res.json({ ok: false, error: 'API Key inválida o sin permisos (HTTP ' + resp.status + ')' });
    }
    // Cualquier otro status (400, 422, etc) indica que la key es válida pero el payload está vacío
    res.json({ ok: true, mensaje: 'API Key válida — conexión OK', httpStatus: resp.status });
  } catch (err) {
    console.error('[HAULMER TEST ERROR]', err.message);
    res.json({ ok: false, error: 'Error de conexión: ' + err.message });
  }
});

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
  if (checkEmpresaAdminOnly(req, res, lote.empresa_id)) return;

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

  let sql = `SELECT m.id, m.empresa_id, m.fecha, m.monto, m.monto_total, m.rut, m.rut_normalizado,
             m.razon_social, m.nombre_origen, m.tipo_dte, m.estado, m.banco_cartola, m.email_receptor,
             m.giro, m.direccion, m.comuna, m.ciudad, m.lote_id, m.fecha_facturacion, m.id_transferencia,
             lf.nombre as lote_nombre
             FROM movimientos m
             LEFT JOIN lotes_facturacion lf ON m.lote_id = lf.lote_id
             WHERE m.estado = 'facturado'`;
  const params = [];

  if (empresaId) { sql += ' AND m.empresa_id = ?'; params.push(empresaId); }
  if (fecha_desde) { sql += ' AND m.fecha >= ?'; params.push(fecha_desde); }
  if (fecha_hasta) { sql += ' AND m.fecha <= ?'; params.push(fecha_hasta); }
  if (tipo_dte) { sql += ' AND m.tipo_dte = ?'; params.push(parseInt(tipo_dte)); }

  // Buscador: por nombre, RUT o monto
  if (buscar) {
    const term = buscar.trim();
    // Detectar si es búsqueda por monto (solo números y puntos/comas)
    if (/^[\d.,]+$/.test(term)) {
      const montoNum = parseFloat(term.replace(/\./g, '').replace(',', '.'));
      if (!isNaN(montoNum)) {
        sql += ' AND (m.monto_total = ? OR m.monto = ?)';
        params.push(montoNum, montoNum);
      }
    } else {
      sql += ` AND (m.razon_social LIKE ? OR m.nombre_origen LIKE ? OR m.rut LIKE ? OR m.rut_normalizado LIKE ?)`;
      const like = `%${term}%`;
      params.push(like, like, like, like);
    }
  }

  // Count total
  const countSql = sql.replace(/SELECT .+ FROM/, 'SELECT COUNT(*) as total FROM');
  const total = db.prepare(countSql).get(...params)?.total || 0;

  sql += ' ORDER BY m.fecha_facturacion DESC, m.id DESC LIMIT ? OFFSET ?';
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

  const empresaIds = empresaId ? [empresaId] : ['tg-inversiones', 'mt-inversiones', 'ts-capital'];

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

// ── Advanced Dashboard stats (monthly breakdown, trends) ────────────────────
app.get('/api/dashboard/advanced', requireAuth, (req, res) => {
  const empresaId = filterByEmpresa(req);
  const empresaIds = empresaId ? [empresaId] : ['tg-inversiones', 'mt-inversiones', 'ts-capital'];

  // Last 12 months data
  const months = [];
  const now = new Date();
  for (let i = 11; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push({
      year: d.getFullYear(),
      month: d.getMonth() + 1,
      label: d.toLocaleDateString('es-CL', { month: 'short', year: '2-digit' }),
      start: `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-01`,
      end: `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-31`
    });
  }

  // Helper to normalize date to YYYY-MM for grouping
  function extractYM(fecha) {
    if (!fecha) return null;
    const s = String(fecha).trim().split('T')[0].split(' ')[0];
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0, 7);
    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
      const [d, m, y] = s.split('/');
      return `${y}-${m.padStart(2, '0')}`;
    }
    if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(s)) {
      const [d, m, y] = s.split('-');
      return `${y}-${m.padStart(2, '0')}`;
    }
    return null;
  }

  // Monthly DTE emissions (facturados) per empresa
  const monthlyByEmpresa = {};
  const monthlyTotals = {};
  // Monthly monto facturado
  const monthlyMonto = {};

  for (const eid of empresaIds) {
    monthlyByEmpresa[eid] = {};
    const facturados = db.prepare(
      "SELECT fecha, monto FROM movimientos WHERE empresa_id = ? AND estado = 'facturado'"
    ).all(eid);

    for (const f of facturados) {
      const ym = extractYM(f.fecha);
      if (!ym) continue;
      monthlyByEmpresa[eid][ym] = (monthlyByEmpresa[eid][ym] || 0) + 1;
      monthlyTotals[ym] = (monthlyTotals[ym] || 0) + 1;
      monthlyMonto[ym] = (monthlyMonto[ym] || 0) + (f.monto || 0);
    }
  }

  // Build monthly series
  const monthlySeries = months.map(m => {
    const ym = `${m.year}-${String(m.month).padStart(2,'0')}`;
    const byEmpresa = {};
    for (const eid of empresaIds) {
      byEmpresa[eid] = monthlyByEmpresa[eid]?.[ym] || 0;
    }
    return {
      label: m.label,
      ym,
      total: monthlyTotals[ym] || 0,
      monto: monthlyMonto[ym] || 0,
      byEmpresa
    };
  });

  // Top 10 clients by facturado count
  const topClients = db.prepare(`
    SELECT rut, razon_social, nombre_origen, empresa_id, COUNT(*) as cnt, SUM(monto) as total_monto
    FROM movimientos
    WHERE estado = 'facturado' AND empresa_id IN (${empresaIds.map(() => '?').join(',')})
    GROUP BY rut_normalizado
    ORDER BY cnt DESC
    LIMIT 10
  `).all(...empresaIds);

  // DTE type breakdown
  const dteBreakdown = db.prepare(`
    SELECT tipo_dte, COUNT(*) as cnt, SUM(monto) as total_monto
    FROM movimientos
    WHERE estado = 'facturado' AND empresa_id IN (${empresaIds.map(() => '?').join(',')})
    GROUP BY tipo_dte
  `).all(...empresaIds);

  // Recent lotes (last 10)
  const recentLotes = db.prepare(`
    SELECT lote_id, empresa_id, cantidad, monto_total, estado, metodo, created_at
    FROM lotes_facturacion
    WHERE empresa_id IN (${empresaIds.map(() => '?').join(',')})
    ORDER BY created_at DESC
    LIMIT 10
  `).all(...empresaIds);

  // Estado breakdown
  const estadoBreakdown = {};
  for (const eid of empresaIds) {
    estadoBreakdown[eid] = db.prepare(`
      SELECT estado, COUNT(*) as cnt FROM movimientos WHERE empresa_id = ? GROUP BY estado
    `).all(eid);
  }

  // Estado breakdown
  // ── KPI Mes actual (DTE emitidos vía API) ────────────────────────────────────
  // nowCL() → es-CL → "DD-MM-YYYY, HH:MM:SS a. m."
  // pos 3-4 (0-idx) = mes MM  |  pos 6-9 (0-idx) = año YYYY
  const nowStr   = nowCL();
  const mesMM    = nowStr.slice(3, 5);   // "04"
  const anioYYYY = nowStr.slice(6, 10);  // "2026"
  const mesActualYM = `${anioYYYY}-${mesMM}`;

  // CRITERIO CORRECTO: solo lotes cuyo estado indica emisión API exitosa
  //   'emitido'  → SimpleFactura o Haulmer (todos los docs del lote OK)
  //   'parcial'  → Haulmer con algunos docs OK (los movimientos OK tienen estado='facturado')
  // Excluye: 'facturado_manual', 'exportado_manual', 'error', 'pendiente'
  //
  // Fecha de referencia: l.updated_at del lote (cuándo se marcó como 'emitido')
  // También en formato es-CL → mismo substr(pos 4,2) y substr(pos 7,4)
  // Criterio de fecha: m.fecha_facturacion (set al momento exacto de emisión API)
  // NO usamos l.updated_at porque puede modificarse después (ej: quitar-movimiento)
  // m.fecha_facturacion para API-emitidos = nowCL() → es-CL "DD-MM-YYYY, ..."
  // Para manuales = m.fecha en YYYY-MM-DD → no coincide con substr(pos4,2)="04"
  const DTE_MES_SQL = `
    SELECT COUNT(m.id) as cnt, COALESCE(SUM(m.monto), 0) as monto
    FROM movimientos m
    JOIN lotes_facturacion l ON m.lote_id = l.lote_id
    WHERE m.empresa_id IN (${empresaIds.map(() => '?').join(',')})
      AND m.estado = 'facturado'
      AND l.estado IN ('emitido', 'parcial')
      AND substr(m.fecha_facturacion, 4, 2) = ?
      AND substr(m.fecha_facturacion, 7, 4) = ?
  `;
  const dteMesActualRows = db.prepare(DTE_MES_SQL).get(...empresaIds, mesMM, anioYYYY);

  const dteMesActual   = dteMesActualRows?.cnt   || 0;
  const montoMesActual = dteMesActualRows?.monto || 0;

  // Breakdown por empresa
  const dteMesActualPorEmpresa = {};
  for (const eid of empresaIds) {
    const r = db.prepare(`
      SELECT COUNT(m.id) as cnt, COALESCE(SUM(m.monto), 0) as monto
      FROM movimientos m
      JOIN lotes_facturacion l ON m.lote_id = l.lote_id
      WHERE m.empresa_id = ?
        AND m.estado = 'facturado'
        AND l.estado IN ('emitido', 'parcial')
        AND substr(m.fecha_facturacion, 4, 2) = ?
        AND substr(m.fecha_facturacion, 7, 4) = ?
    `).get(eid, mesMM, anioYYYY);
    dteMesActualPorEmpresa[eid] = { cnt: r?.cnt || 0, monto: r?.monto || 0 };
  }

  res.json({
    monthlySeries,
    topClients,
    dteBreakdown,
    recentLotes,
    estadoBreakdown,
    empresaIds,
    dteMesActual,
    montoMesActual,
    dteMesActualPorEmpresa,
    mesActualYM,
    mesMM,
    anioYYYY
  });
});

// ── Diagnóstico DTE mes actual (solo admin) ──────────────────────────────────
// GET /api/diagnostico/dte-mes-actual?empresa_id=tg-inversiones
// Devuelve los lotes y movimientos que componen el KPI para depurar discrepancias
app.get('/api/diagnostico/dte-mes-actual', requireAuth, (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo admin' });
  const empresaId = req.query.empresa_id || null;
  const empresaIds = empresaId ? [empresaId] : ['tg-inversiones','mt-inversiones','ts-capital','vanher-capital'];

  const nowStr   = nowCL();
  const mesMM    = nowStr.slice(3, 5);
  const anioYYYY = nowStr.slice(6, 10);

  // Lotes emitidos del mes con sus cantidades
  const lotes = db.prepare(`
    SELECT l.lote_id, l.empresa_id, l.nombre, l.cantidad, l.monto_total,
           l.estado, l.metodo, l.created_at, l.updated_at,
           COUNT(m.id) as movs_facturados,
           COALESCE(SUM(m.monto),0) as monto_movs,
           substr(l.updated_at, 4, 2) as mes_lote,
           substr(l.updated_at, 7, 4) as anio_lote
    FROM lotes_facturacion l
    LEFT JOIN movimientos m ON m.lote_id = l.lote_id
      AND m.estado = 'facturado'
      AND substr(m.fecha_facturacion, 4, 2) = ?
      AND substr(m.fecha_facturacion, 7, 4) = ?
    WHERE l.empresa_id IN (${empresaIds.map(() => '?').join(',')})
      AND l.estado IN ('emitido','parcial')
    GROUP BY l.lote_id
    ORDER BY l.updated_at DESC
    LIMIT 100
  `).all(mesMM, anioYYYY, ...empresaIds);

  // Totales por estado de lote (para ver si hay lotes 'emitido' fuera del mes también)
  const estadosLotes = db.prepare(`
    SELECT l.estado, COUNT(*) as lotes, COALESCE(SUM(l.cantidad),0) as cantidad_total,
           substr(l.updated_at, 4, 2) as mes_upd, substr(l.updated_at, 7, 4) as anio_upd
    FROM lotes_facturacion l
    WHERE l.empresa_id IN (${empresaIds.map(() => '?').join(',')})
    GROUP BY l.estado, mes_upd, anio_upd
    ORDER BY anio_upd DESC, mes_upd DESC
  `).all(...empresaIds);

  // Sample de fecha_facturacion para ver formatos reales
  const sampleFechas = db.prepare(`
    SELECT m.empresa_id, m.fecha_facturacion, m.updated_at, l.estado as lote_estado
    FROM movimientos m
    JOIN lotes_facturacion l ON m.lote_id = l.lote_id
    WHERE m.empresa_id IN (${empresaIds.map(() => '?').join(',')})
      AND m.estado = 'facturado'
      AND l.estado IN ('emitido','parcial')
    ORDER BY m.id DESC
    LIMIT 20
  `).all(...empresaIds);

  res.json({
    mesMM, anioYYYY,
    nowStr_ejemplo: nowStr,
    lotes_contados_mes: lotes.filter(l => l.movs_facturados > 0),
    lotes_emitidos_sin_movs_mes: lotes.filter(l => l.movs_facturados === 0),
    estadosLotes,
    sample_fecha_facturacion: sampleFechas
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// MÓDULO: PROCESAMIENTO AUTOMÁTICO DE CARTOLAS CON SIMPLEAPI
// ══════════════════════════════════════════════════════════════════════════════

// Cargar xlsx para parsing server-side
let XLSX_LIB;
try { XLSX_LIB = require('xlsx'); } catch(e) {
  console.warn('[XLSX] Paquete no disponible — instalar con: npm install xlsx');
}

// ── SimpleAPI configuration ───────────────────────────────────────────────────
const SIMPLEAPI_BASE = 'https://api.simpleapi.cl';
// Keys por defecto (hardcoded como fallback); también se pueden sobrescribir vía DB
const SIMPLEAPI_KEYS_DEFAULT = {
  'tg-inversiones': '2131-W810-6394-2111-1765',
  'mt-inversiones': '2128-N940-6394-5813-7213'
};

function getSimpleApiKey(empresaId) {
  try {
    const cfg = getAppData('simpleapi_keys') || {};
    return cfg[empresaId] || SIMPLEAPI_KEYS_DEFAULT[empresaId] || null;
  } catch(e) { return SIMPLEAPI_KEYS_DEFAULT[empresaId] || null; }
}

// Limpiar nombre de glosa Santander: elimina RUT inicial y prefijos "Transf de", "Transf.", etc.
function cleanSantanderName(glosa) {
  if (!glosa) return '';
  return glosa
    .replace(/^0*\d{6,11}[kK]?\s+/i, '')         // RUT al inicio (ej: "026042825K ")
    .replace(/Transferencia\s+de\s+/gi, '')
    .replace(/Transf(?:erencia)?\.?\s+de\s+/gi, '')
    .replace(/Transf(?:erencia)?\.?\s+/gi, '')
    .replace(/^de\s+/i, '')
    .trim()
    .replace(/\s{2,}/g, ' ');
}

// Formatear dígitos de RUT a "XX.XXX.XXX-X"
function formatRutDigits(rutDigits) {
  if (!rutDigits) return '';
  const clean = String(rutDigits).replace(/[^0-9kK]/g, '').toUpperCase();
  if (clean.length < 2) return clean;
  const body = clean.slice(0, -1);
  const dv   = clean.slice(-1);
  const formatted = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return formatted + '-' + dv;
}

// ── Parser BCI (.xls) ─────────────────────────────────────────────────────────
function parseBCICartola(buffer) {
  if (!XLSX_LIB) throw new Error('Módulo xlsx no disponible en el servidor');
  const wb = XLSX_LIB.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const data = XLSX_LIB.utils.sheet_to_json(ws, { header: 1, defval: '' });

  const movimientos = [];
  let headerIdx = -1;
  for (let i = 0; i < Math.min(20, data.length); i++) {
    const rowStr = data[i].map(c => String(c || '')).join('|');
    if (rowStr.includes('Fecha') && rowStr.includes('ID')) { headerIdx = i; break; }
    if (rowStr.includes('ID Transferencia'))               { headerIdx = i; break; }
  }
  const dataStart = headerIdx >= 0 ? headerIdx + 1 : 9;

  for (let i = dataStart; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length < 5) continue;

    // Columna 0: Fecha (string DD/MM/YYYY o serial numérico Excel)
    let fecha = '';
    const fechaRaw = row[0];
    if (typeof fechaRaw === 'number') {
      const d = XLSX_LIB.SSF.parse_date_code(fechaRaw);
      if (d) fecha = `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
    } else {
      const fs = String(fechaRaw || '').trim();
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(fs)) {
        const [dd, mm, yyyy] = fs.split('/');
        fecha = `${yyyy}-${mm.padStart(2,'0')}-${dd.padStart(2,'0')}`;
      } else if (/^\d{4}-\d{2}-\d{2}$/.test(fs)) {
        fecha = fs;
      }
    }
    if (!fecha) continue;

    const idTransf    = String(row[1] || '').trim();
    if (!idTransf || idTransf === 'ID Transferencia') continue;
    const rutRaw      = String(row[2] || '').trim();
    const bancoOrigen = String(row[3] || '').trim();
    const cuentaOrig  = String(row[4] || '').trim();

    let monto = row[5];
    if (typeof monto === 'string') {
      monto = parseFloat(monto.replace(/\./g, '').replace(',', '.').replace(/[$\s]/g, ''));
    }
    if (!monto || monto <= 0) continue;

    const estado = String(row[6] || '').trim();
    if (!estado.toLowerCase().includes('recibida')) continue;

    const nombre = String(row[7] || '').trim();

    movimientos.push({
      id_transferencia: idTransf,
      fecha, monto,
      glosa: nombre,
      rut: rutRaw,                            // con puntos y guión del banco
      nombre_origen: nombre,
      banco_origen: bancoOrigen || 'BCI',
      cuenta_origen: cuentaOrig
    });
  }
  return movimientos;
}

// ── Parser Santander (.xlsx) ──────────────────────────────────────────────────
function parseSantanderCartola(buffer) {
  if (!XLSX_LIB) throw new Error('Módulo xlsx no disponible en el servidor');
  const wb = XLSX_LIB.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const data = XLSX_LIB.utils.sheet_to_json(ws, { header: 1, defval: '' });

  const movimientos = [];
  let headerIdx = -1;
  for (let i = 0; i < Math.min(15, data.length); i++) {
    const cell0 = String(data[i]?.[0] || '');
    if (cell0.includes('mero de Movimiento') || cell0.includes('Número')) { headerIdx = i; break; }
  }
  if (headerIdx === -1) headerIdx = 9;

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length < 3) continue;

    const idRaw = String(row[0] || '').trim();
    if (!idRaw) continue;

    // Fecha DD/MM/YYYY → YYYY-MM-DD
    let fecha = String(row[1] || '').trim();
    if (fecha.includes('/')) {
      const [d, m, y] = fecha.split('/');
      fecha = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
    }

    let monto = row[2];
    if (typeof monto === 'string') {
      monto = parseFloat(monto.replace(/[$.\s]/g, '').replace(',', '.'));
    }
    if (!monto || monto <= 0) continue;

    const glosa = String(row[3] || '').trim();

    // Extraer RUT desde glosa: patrón "0XXXXXXXXXX Transf de NOMBRE"
    let rutDigits = '';
    let nombre    = '';
    const m1 = glosa.match(/^0*(\d{7,9}[0-9kK])\s+Transf[\s.]*(?:de\s+)?(.*)$/i);
    if (m1) {
      rutDigits = m1[1];
      nombre    = m1[2]?.trim() || '';
    } else {
      const m2 = glosa.match(/(\d{7,9}[0-9kK])/);
      if (m2) rutDigits = m2[1];
      nombre = cleanSantanderName(glosa);
    }

    // ID único: si todos ceros usar "S{i}"
    const useId = idRaw.replace(/0/g,'') === '' ? `S${i}` : idRaw;
    // Santander reutiliza números secuenciales cada mes → prefijamos fecha para unicidad entre períodos
    const idConFecha = fecha ? `${fecha}_${useId}` : useId;

    movimientos.push({
      id_transferencia: idConFecha,
      fecha, monto, glosa,
      rut: rutDigits ? formatRutDigits(rutDigits) : '',
      rut_digits: rutDigits,                  // solo dígitos para consulta API
      nombre_origen: nombre,
      banco_origen: 'No específica',
      cuenta_origen: '999999'
    });
  }
  return movimientos;
}

// ── Parser Banco Estado (.xlsx) ───────────────────────────────────────────────
// Formato: sheet "Transferencias" con columnas:
// N° Operación | Fecha - Hora | Cuenta Destino | Alias Destino | Rut Origen | Banco Origen | Nombre Origen | Cuenta Origen | Monto
function parseBancoEstadoCartola(buffer) {
  if (!XLSX_LIB) throw new Error('Módulo xlsx no disponible en el servidor');
  const wb = XLSX_LIB.read(buffer, { type: 'buffer' });

  // Buscar sheet "Transferencias" o usar el primero disponible
  const sheetName = wb.SheetNames.includes('Transferencias') ? 'Transferencias' : wb.SheetNames[wb.SheetNames.length - 1];
  const ws = wb.Sheets[sheetName];
  const data = XLSX_LIB.utils.sheet_to_json(ws, { header: 1, defval: '' });

  const movimientos = [];
  let headerIdx = -1;

  // Buscar fila de encabezado
  for (let i = 0; i < Math.min(5, data.length); i++) {
    const rowStr = data[i].join('|');
    if (rowStr.includes('Rut Origen') || rowStr.includes('N° Operación') || rowStr.includes('Operaci')) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx === -1) headerIdx = 0;

  // Mapear índices de columnas desde la fila de cabecera
  const headers = data[headerIdx].map(h => String(h || '').trim());
  const colIdx = (name) => headers.findIndex(h => h.includes(name));

  const idxId     = colIdx('Operaci');   // N° Operación
  const idxFecha  = colIdx('Fecha');     // Fecha - Hora
  const idxRut    = colIdx('Rut Origen');
  const idxBanco  = colIdx('Banco Origen');
  const idxNombre = colIdx('Nombre Origen');
  const idxCuenta = colIdx('Cuenta Origen');
  const idxMonto  = colIdx('Monto');

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row || !row.length) continue;

    const idRaw = String(row[idxId] !== undefined ? row[idxId] : '').trim();
    if (!idRaw) continue;

    // Fecha: "DD/MM/YYYY HH:MM" → YYYY-MM-DD
    let fecha = '';
    const fechaRaw = row[idxFecha];
    if (typeof fechaRaw === 'number') {
      const d = XLSX_LIB.SSF.parse_date_code(fechaRaw);
      if (d) fecha = `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
    } else {
      const fs = String(fechaRaw || '').trim();
      const matchFecha = fs.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
      if (matchFecha) fecha = `${matchFecha[3]}-${matchFecha[2]}-${matchFecha[1]}`;
    }
    if (!fecha) continue;

    let monto = row[idxMonto];
    if (typeof monto === 'string') {
      monto = parseFloat(monto.replace(/\./g, '').replace(',', '.').replace(/[$\s]/g, ''));
    }
    monto = parseFloat(monto) || 0;
    if (monto <= 0) continue;

    const rutRaw      = idxRut >= 0 ? String(row[idxRut] || '').trim() : '';
    const bancoOrigen = idxBanco >= 0 ? String(row[idxBanco] || '').trim() : '';
    const nombre      = idxNombre >= 0 ? String(row[idxNombre] || '').trim() : '';
    const cuenta      = idxCuenta >= 0 ? String(row[idxCuenta] || '').trim() : '';

    movimientos.push({
      id_transferencia: idRaw,
      fecha,
      monto,
      glosa: nombre,
      rut: rutRaw,
      nombre_origen: nombre,
      banco_origen: bancoOrigen || 'Banco Estado',
      cuenta_origen: cuenta
    });
  }
  return movimientos;
}

// ── Parser Banco Chile (.xls) ─────────────────────────────────────────────────
// Formato sheet "TEF Empresa":
// Row ~10 (header): ["","Fecha y hora","Nombre o razón social origen","Rut origen","Banco Origen","Cuenta Origen","Tipo de operación","Cuenta Destino","Monto","ID Transacción","Tipo Moneda","Tipo Operador","Comentario"]
// Col 0 siempre vacía (offset de 1). Banco Chile entrega nombre directo del comprador.
function parseBancoChileCartola(buffer) {
  if (!XLSX_LIB) throw new Error('Módulo xlsx no disponible en el servidor');
  const wb = XLSX_LIB.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const data = XLSX_LIB.utils.sheet_to_json(ws, { header: 1, defval: '' });

  const movimientos = [];
  let headerIdx = -1;
  for (let i = 0; i < Math.min(20, data.length); i++) {
    const rowStr = data[i].map(c => String(c || '')).join('|');
    if (rowStr.includes('Fecha y hora') && rowStr.includes('Nombre o raz')) { headerIdx = i; break; }
    if (rowStr.includes('ID Transacci') && rowStr.includes('Monto'))         { headerIdx = i; break; }
  }
  const dataStart = headerIdx >= 0 ? headerIdx + 1 : 11;

  for (let i = dataStart; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length < 10) continue;

    // Col 1: "Fecha y hora" → "DD-MM-YYYY HH:MM"
    const fechaRaw = String(row[1] || '').trim();
    if (!fechaRaw) continue;
    let fecha = '';
    const mf = fechaRaw.match(/^(\d{2})-(\d{2})-(\d{4})/);
    if (mf) fecha = `${mf[3]}-${mf[2]}-${mf[1]}`;
    if (!fecha) continue;

    // Col 9: "ID Transacción" → "TEF_IPE..."
    const idTransf = String(row[9] || '').trim();
    if (!idTransf) continue;

    // Col 2: "Nombre o razón social origen" — entregado directamente por el banco
    const nombre = String(row[2] || '').trim();

    // Col 3: "Rut origen" → "14.318.511-7" (ya con puntos y guión)
    const rutRaw = String(row[3] || '').trim();

    // Col 4: "Banco Origen"
    const bancoOrigen = String(row[4] || '').trim();

    // Col 5: "Cuenta Origen"
    const cuentaOrig = String(row[5] || '').trim();

    // Col 8: "Monto" → "$350.000"
    let monto = row[8];
    if (typeof monto === 'string') {
      monto = parseFloat(monto.replace(/[$.\s]/g, '').replace(',', '.'));
    }
    monto = parseFloat(monto) || 0;
    if (monto <= 0) continue;

    // Col 12: "Comentario" → glosa
    const glosa = String(row[12] || '').trim();

    movimientos.push({
      id_transferencia: idTransf,
      fecha,
      monto,
      glosa: glosa || nombre,
      rut: rutRaw,
      nombre_origen: nombre,
      banco_origen: bancoOrigen || 'Banco Chile',
      cuenta_origen: cuentaOrig
    });
  }
  return movimientos;
}

// ── Consultar RUT en SimpleAPI ────────────────────────────────────────────────
async function consultarRUTSimpleAPI(rutNorm, apiKey) {
  if (!apiKey || !rutNorm || rutNorm.length < 3) return null;
  // Intentar dos variantes del endpoint (la documentación puede diferir entre versiones)
  const endpoints = [
    `${SIMPLEAPI_BASE}/v1/rut/${encodeURIComponent(rutNorm)}`,
    `https://www.simpleapi.cl/api/Rut/${encodeURIComponent(rutNorm)}`
  ];
  for (const url of endpoints) {
    try {
      const resp = await fetch(url, {
        headers: { 'Authorization': apiKey, 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(8000)
      });
      if (resp.status === 404) { console.log(`[SIMPLEAPI] RUT ${rutNorm} no encontrado (404)`); return null; }
      if (!resp.ok) {
        console.log(`[SIMPLEAPI] RUT ${rutNorm} → HTTP ${resp.status} en ${url}`);
        continue; // intentar siguiente endpoint
      }
      const data = await resp.json();
      console.log(`[SIMPLEAPI] RUT ${rutNorm} → ${JSON.stringify(data).substring(0,200)}`);
      return data;
    } catch(e) {
      console.warn(`[SIMPLEAPI] Error en ${url}: ${e.message}`);
    }
  }
  return null;
}

// Normalizar respuesta SimpleAPI a campos internos
function normalizarRespuestaSimpleAPI(data) {
  if (!data) return null;
  return {
    razon_social: data.razonSocial || data.nombre || data.name || data.RazonSocial || '',
    giro:         data.giro || data.actividad || data.Giro || data.Actividad || '',
    direccion:    data.direccion || data.address || data.Direccion || '',
    comuna:       data.comuna || data.Comuna || '',
    ciudad:       data.ciudad || data.Ciudad || '',
    email:        data.email || data.correo || data.Email || ''
  };
}

// ── Endpoint principal: procesar cartola server-side con SimpleAPI ─────────────
app.post('/api/movimientos/cargar-y-procesar', requireAuth, upload.single('cartola'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No se subió archivo' });
    if (!XLSX_LIB) return res.status(500).json({ error: 'Módulo xlsx no instalado — ejecutar: npm install xlsx' });

    const empresaId = req.user.role === 'admin' ? (req.body.empresa_id || '') : req.user.empresa;
    if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });
    if (checkEmpresaAdminOnly(req, res, empresaId)) return;

    // Auto-detectar banco si no viene explícito
    let bancoCartola = (req.body.banco || '').toUpperCase().trim();
    if (!bancoCartola) {
      const fname = req.file.originalname.toLowerCase();
      if (fname.includes('santander'))                            bancoCartola = 'SANTANDER';
      else if (fname.includes('banco_estado') || fname.includes('bancoestado') || fname.includes('banco estado'))
                                                                  bancoCartola = 'BANCO ESTADO';
      else if (fname.includes('banco_chile') || fname.includes('bancochile') || fname.includes('banco chile') || fname.includes('bchile') || fname.includes('chile'))
                                                                  bancoCartola = 'BANCO CHILE';
      else if (fname.includes('bci'))                             bancoCartola = 'BCI';
      else bancoCartola = fname.endsWith('.xlsx') ? 'SANTANDER' : 'BCI';
    }
    // Normalizar variantes
    if (['BANCOESTADO','BANCO_ESTADO','ESTADO'].includes(bancoCartola)) bancoCartola = 'BANCO ESTADO';
    if (['CHILE','BCHILE','BANCO_CHILE','BANCOCHILE'].includes(bancoCartola)) bancoCartola = 'BANCO CHILE';

    // Parsear cartola
    let movimientosRaw = [];
    try {
      if (bancoCartola === 'SANTANDER')         movimientosRaw = parseSantanderCartola(req.file.buffer);
      else if (bancoCartola === 'BANCO ESTADO') movimientosRaw = parseBancoEstadoCartola(req.file.buffer);
      else if (bancoCartola === 'BANCO CHILE')  movimientosRaw = parseBancoChileCartola(req.file.buffer);
      else                                      movimientosRaw = parseBCICartola(req.file.buffer);
    } catch(parseErr) {
      return res.status(422).json({ error: 'Error al parsear archivo: ' + parseErr.message });
    }

    if (movimientosRaw.length === 0) {
      return res.status(422).json({
        error: `No se encontraron movimientos válidos en la cartola ${bancoCartola}. ` +
               'Verifica que el archivo corresponda al banco seleccionado y tenga el formato correcto.'
      });
    }

    const apiKey    = getSimpleApiKey(empresaId);
    const empresas  = getAppData('empresas') || {};
    const empConf   = empresas[empresaId];
    const config    = getAppData('config') || {};
    const now       = nowCL();

    // Cargar clientes existentes en memoria (para matching rápido)
    const allClientes = db.prepare('SELECT * FROM clientes WHERE empresa_id = ?').all(empresaId);
    const clienteMap  = new Map();
    for (const c of allClientes) {
      if (c.rut_normalizado) clienteMap.set(c.rut_normalizado, c);
    }

    // ── Consultar SimpleAPI para RUTs de Santander desconocidos ──────────────
    const simpleApiResults = {};  // rutNorm → datos normalizados
    let simpleApiConsultados = 0;

    if (bancoCartola === 'SANTANDER' && apiKey) {
      // Recopilar RUTs únicos que no están en nuestra base
      const rutsNuevos = new Set();
      for (const mov of movimientosRaw) {
        const rutNorm = normalizeRut(mov.rut || mov.rut_digits || '');
        if (rutNorm && !clienteMap.has(rutNorm)) rutsNuevos.add(rutNorm);
      }

      // Consultar en lotes de 3 (respetar rate limit ≤3 req/seg de SimpleAPI)
      const rutArr = [...rutsNuevos];
      for (let i = 0; i < rutArr.length; i += 3) {
        const batch = rutArr.slice(i, i + 3);
        const batchResults = await Promise.allSettled(
          batch.map(rut => consultarRUTSimpleAPI(rut, apiKey).then(d => ({ rut, data: d })))
        );
        for (const br of batchResults) {
          if (br.status === 'fulfilled' && br.value.data) {
            const normalized = normalizarRespuestaSimpleAPI(br.value.data);
            if (normalized?.razon_social) {
              simpleApiResults[br.value.rut] = normalized;
              simpleApiConsultados++;
            }
          }
        }
        if (i + 3 < rutArr.length) await new Promise(r => setTimeout(r, 400));
      }
      console.log(`[SIMPLEAPI] ${simpleApiConsultados} RUTs encontrados de ${rutArr.size || rutArr.length} consultados`);
    }

    // ── Procesar e insertar movimientos ───────────────────────────────────────
    const checkDup    = db.prepare('SELECT id, estado, created_at, lote_carga_id FROM movimientos WHERE id_compuesto = ? AND empresa_id = ?');
    const insertMov   = db.prepare(`
      INSERT INTO movimientos
        (empresa_id, id_transferencia, fecha, monto, glosa, rut, rut_normalizado,
         nombre_origen, banco_origen, banco_cartola, cuenta_origen, id_compuesto,
         estado, tipo_dte, razon_social, giro, direccion, comuna, ciudad, email_receptor,
         nombre_item, descripcion_item, precio, monto_exento, monto_total,
         fecha_carga, created_at, updated_at, lote_carga_id)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const insertCliente = db.prepare(`
      INSERT OR IGNORE INTO clientes
        (empresa_id, tipo, rut, rut_normalizado, razon_social, giro, direccion,
         comuna, ciudad, nombre, email, telefono, representante_legal, rut_representante,
         created_at, updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);

    const loteCargaId = `${empresaId}-${bancoCartola}-${Date.now()}`.toLowerCase().replace(/\s/g, '-');
    let nuevos = 0, duplicados = 0, errores = 0, clientesNuevos = 0;
    const resultDetails = [];
    let primerDupFecha = null, primerDupLote = null;

    db.transaction(() => {
      for (const mov of movimientosRaw) {
        try {
          const idTransf    = String(mov.id_transferencia || '').trim();
          const idCompuesto = `${empresaId}_${idTransf}_${bancoCartola}`;

          // ── Verificar duplicado ──────────────────────────────────────────
          const existing = checkDup.get(idCompuesto, empresaId);
          if (existing) {
            duplicados++;
            if (!primerDupFecha) { primerDupFecha = existing.created_at; primerDupLote = existing.lote_carga_id; }
            resultDetails.push({ id_compuesto: idCompuesto, status: 'duplicado', estado_previo: existing.estado, primera_carga: existing.created_at });
            continue;
          }

          // ── Determinar tipo de cliente y RUT ────────────────────────────
          const rutNorm = normalizeRut(mov.rut || mov.rut_digits || '');
          let estado = 'pendiente', tipoDte = null;
          let razonSocial = '', giro = '', direccion = '', comuna = '', ciudad = '', emailReceptor = '';
          let clienteEsNuevo = false, fuenteRazonSocial = 'cartola';

          // RUT o nombre excluido: empresas propias, representantes o personas internas
          if (esMovimientoInterno(rutNorm, mov.nombre_origen, empresaId)) {
            estado = 'interno';
          } else if (rutNorm) {
            tipoDte = getTipoDte(rutNorm, empConf);
            const clienteExistente = clienteMap.get(rutNorm);

            if (clienteExistente) {
              // ✅ Cliente conocido en BD
              estado       = 'listo';
              razonSocial  = clienteExistente.razon_social || '';
              giro         = clienteExistente.giro         || '';
              direccion    = clienteExistente.direccion     || '';
              comuna       = clienteExistente.comuna        || '';
              ciudad       = clienteExistente.ciudad        || '';
              emailReceptor= clienteExistente.email         || '';
              fuenteRazonSocial = 'bd';
            } else {
              // 🆕 Cliente nuevo — determinar fuente del nombre
              clienteEsNuevo = true;
              const apiData  = simpleApiResults[rutNorm];

              if (apiData?.razon_social) {
                // ✅ SimpleAPI devolvió datos
                razonSocial   = apiData.razon_social;
                giro          = apiData.giro         || '';
                direccion     = apiData.direccion     || '';
                comuna        = apiData.comuna        || '';
                ciudad        = apiData.ciudad        || '';
                emailReceptor = apiData.email         || '';
                estado        = 'listo';
                fuenteRazonSocial = 'simpleapi';
              } else if (bancoCartola === 'BCI' || bancoCartola === 'BANCO CHILE') {
                // BCI y Banco Chile entregan nombre y RUT completo directamente
                razonSocial = (mov.nombre_origen || '').trim();
                estado      = tipoDte === 41 ? 'listo' : 'pendiente';
                fuenteRazonSocial = bancoCartola === 'BANCO CHILE' ? 'cartola_banco_chile' : 'cartola_bci';
              } else {
                // Santander sin datos API → limpiar nombre de glosa
                razonSocial = cleanSantanderName(mov.glosa || '') || (mov.nombre_origen || '').trim();
                estado      = tipoDte === 41 ? 'listo' : 'pendiente';
                fuenteRazonSocial = 'glosa_limpia';
              }

              // Guardar nuevo cliente en BD
              const tipo       = (tipoDte === 34) ? 'empresa' : 'persona';
              const rutFormato = formatRutDigits(rutNorm) || mov.rut || '';
              insertCliente.run(
                empresaId, tipo, rutFormato, rutNorm,
                razonSocial, giro, direccion, comuna, ciudad,
                razonSocial, emailReceptor, '', '', '',
                now, now
              );
              // Agregar al mapa para evitar duplicados dentro del mismo lote
              clienteMap.set(rutNorm, {
                rut_normalizado: rutNorm, rut: rutFormato,
                razon_social: razonSocial, tipo,
                giro, direccion, comuna, ciudad, email: emailReceptor
              });
              clientesNuevos++;
            }
          } else {
            // Sin RUT: usar nombre limpio de la glosa
            razonSocial = cleanSantanderName(mov.glosa || '') || (mov.nombre_origen || '').trim();
            estado = 'pendiente';
          }

          const monto          = parseFloat(mov.monto) || 0;
          const nombreItem     = config.nombre_item_default   || 'Venta paquete activo digital';
          const descripcionItem= `${config.descripcion_item_default || 'Venta paquete activo digital'} Banco ${bancoCartola}`;

          insertMov.run(
            empresaId, idTransf, mov.fecha || '', monto,
            mov.glosa || '', mov.rut || '', rutNorm,
            mov.nombre_origen || '', mov.banco_origen || '', bancoCartola,
            mov.cuenta_origen || '', idCompuesto,
            estado, tipoDte,
            razonSocial.substring(0,100),
            giro.substring(0,80), direccion.substring(0,100),
            comuna, ciudad, emailReceptor,
            nombreItem, descripcionItem,
            monto, monto, monto,   // precio, monto_exento, monto_total
            now, now, now, loteCargaId
          );
          nuevos++;
          resultDetails.push({
            id_compuesto: idCompuesto,
            status: estado,
            rut: rutNorm,
            razon_social: razonSocial,
            tipo_dte: tipoDte,
            cliente_nuevo: clienteEsNuevo,
            fuente: fuenteRazonSocial
          });
        } catch(rowErr) {
          errores++;
          console.error('[CARTOLA ROW ERR]', rowErr.message, mov);
          resultDetails.push({ id_transferencia: mov.id_transferencia, status: 'error', error: rowErr.message });
        }
      }
    })();

    res.json({
      ok: true,
      banco: bancoCartola,
      total: movimientosRaw.length,
      nuevos, duplicados, errores,
      clientes_nuevos: clientesNuevos,
      simpleapi_consultados: simpleApiConsultados,
      lote_carga_id: loteCargaId,
      filename: req.file.originalname,
      primera_carga_dup: primerDupFecha,   // fecha de primera carga de los duplicados
      primer_lote_dup: primerDupLote,       // lote_carga_id del primer duplicado
      results: resultDetails.slice(0, 200)   // limitar payload
    });
  } catch(err) {
    console.error('[CARGAR-Y-PROCESAR FATAL]', err);
    res.status(500).json({ error: 'Error al procesar cartola: ' + err.message });
  }
});

// ── Consulta manual de RUT en SimpleAPI (desde la UI) ────────────────────────
app.post('/api/simpleapi/consultar-rut', requireAuth, async (req, res) => {
  const { rut, empresa_id } = req.body;
  if (!rut) return res.status(400).json({ error: 'RUT requerido' });
  const empresaId = req.user.role === 'admin' ? (empresa_id || req.user.empresa) : req.user.empresa;
  const apiKey = getSimpleApiKey(empresaId);
  if (!apiKey) return res.status(400).json({ error: 'SimpleAPI no configurado para esta empresa' });

  const rutNorm = normalizeRut(rut);
  const rawData = await consultarRUTSimpleAPI(rutNorm, apiKey);
  if (!rawData) return res.json({ ok: false, message: 'RUT no encontrado en SimpleAPI o sin datos disponibles' });

  const normalized = normalizarRespuestaSimpleAPI(rawData);
  res.json({ ok: true, rut_normalizado: rutNorm, data: normalized, raw: rawData });
});

// ── Exportar base de datos en formato Excel original ─────────────────────────
app.get('/api/exportar/base-datos/:empresa_id', requireAuth, (req, res) => {
  if (!XLSX_LIB) return res.status(500).json({ error: 'Módulo xlsx no disponible' });
  const empresaId = req.params.empresa_id;
  if (req.user.role !== 'admin' && req.user.empresa !== empresaId) {
    return res.status(403).json({ error: 'Sin acceso a esta empresa' });
  }

  const wb = XLSX_LIB.utils.book_new();

  // ── Hoja RECIBIDAS ──────────────────────────────────────────────────────────
  const movHeaders = ['ID Transferencia','Fecha','Rut Empresa','Rut Persona','Banco Origen','Cuenta Origen','Monto','Nombre Origen','Estado','Otros','Cartola'];
  const movs = db.prepare('SELECT * FROM movimientos WHERE empresa_id = ? ORDER BY fecha ASC, id ASC').all(empresaId);
  const movRows = [movHeaders];
  for (const m of movs) {
    const rutNorm = m.rut_normalizado || '';
    const isEmp   = rutNorm ? parseInt(rutNorm.slice(0,-1)) >= 50000000 : false;
    const fechaFmt = formatoFechaDDMMYYYY(m.fecha);
    movRows.push([
      m.id_transferencia || '',
      fechaFmt,
      isEmp ? (m.rut || '') : null,
      !isEmp ? (m.rut || '') : null,
      m.banco_origen   || '',
      m.cuenta_origen  || '',
      m.monto          || 0,
      m.nombre_origen  || '',
      m.estado         || 'pendiente',
      null,
      m.banco_cartola  || ''
    ]);
  }
  XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet(movRows), 'RECIBIDAS');

  // ── Hoja DME RECIBIDAS (Directorio Maestro Empresas) ───────────────────────
  const dmeH = ['Rut Empresa','Razón Social','Giro','Dirección','Comuna','Ciudad','Nombre','Correo','Teléfono','Representante Legal','Rut Representante Legal'];
  const empresasC = db.prepare("SELECT * FROM clientes WHERE empresa_id=? AND tipo='empresa' ORDER BY razon_social").all(empresaId);
  const dmeRows = [dmeH, ...empresasC.map(c => [c.rut,c.razon_social,c.giro,c.direccion,c.comuna,c.ciudad,c.nombre||c.razon_social,c.email,c.telefono,c.representante_legal,c.rut_representante])];
  XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet(dmeRows), 'DME RECIBIDAS');

  // ── Hoja RNE RECIBIDAS (vacía — por compatibilidad) ────────────────────────
  XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet([dmeH]), 'RNE RECIBIDAS');

  // ── Hoja DMP RECIBIDAS (Directorio Maestro Personas) ───────────────────────
  const dmpH = ['Rut Persona','Razón Social','Giro','Dirección','Comuna','Ciudad','Nombre','Correo','Teléfono'];
  const personasC = db.prepare("SELECT * FROM clientes WHERE empresa_id=? AND tipo='persona' ORDER BY razon_social").all(empresaId);
  const dmpRows = [dmpH, ...personasC.map(c => [c.rut,c.razon_social,c.giro,c.direccion,c.comuna,c.ciudad,c.nombre||c.razon_social,c.email,c.telefono])];
  XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet(dmpRows), 'DMP RECIBIDAS');

  // ── Hoja RNP RECIBIDAS (vacía — por compatibilidad) ────────────────────────
  XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet([dmpH]), 'RNP RECIBIDAS');

  // ── Hojas vacías de ENVIADAS para mantener estructura ──────────────────────
  const enviadaH = [['ID Transferencia','Fecha','Rut Empresa','Rut Persona','Banco Destino','Cuenta Destino','Monto','Nombre Destino','Estado','Otros','Cartola']];
  for (const sname of ['ENVIADAS','DME ENVIADAS','RNE ENVIADAS','DMP ENVIADAS','RNP ENVIADAS']) {
    XLSX_LIB.utils.book_append_sheet(wb, XLSX_LIB.utils.aoa_to_sheet(enviadaH), sname);
  }

  const xlsBuf = XLSX_LIB.write(wb, { type: 'buffer', bookType: 'xlsx' });
  const emp    = getAppData('empresas')?.[empresaId];
  const nombre = (emp?.nombre || empresaId).replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_\-]/g,'');
  const fecha  = todayCL().replace(/-/g,'');

  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="Base_Datos_${nombre}_${fecha}.xlsx"`);
  res.send(xlsBuf);
});

// ── Importar Excel Base de Datos existente ────────────────────────────────────
app.post('/api/importar/base-datos', requireAuth, upload.single('base'), (req, res) => {
  if (!XLSX_LIB) return res.status(500).json({ error: 'Módulo xlsx no disponible' });
  if (!req.file)  return res.status(400).json({ error: 'No se subió archivo' });

  const empresaId = req.user.role === 'admin' ? (req.body.empresa_id || '') : req.user.empresa;
  if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });

  let wb;
  try {
    wb = XLSX_LIB.read(req.file.buffer, { type: 'buffer' });
  } catch(e) {
    return res.status(422).json({ error: 'Error leyendo Excel: ' + e.message });
  }

  const now = nowCL();
  let importadosClientes = 0, omitidosClientes = 0;
  let importadosMov = 0, omitidosMov = 0;

  const checkCliente  = db.prepare('SELECT id FROM clientes WHERE rut_normalizado=? AND empresa_id=?');
  const insertCliente = db.prepare(`
    INSERT OR IGNORE INTO clientes
      (empresa_id, tipo, rut, rut_normalizado, razon_social, giro, direccion,
       comuna, ciudad, nombre, email, telefono, representante_legal, rut_representante, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  // Helper: importar hoja de clientes
  function importarHojaClientes(sheetName, tipo) {
    if (!wb.SheetNames.includes(sheetName)) return;
    const rows = XLSX_LIB.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1, defval: '' });
    db.transaction(() => {
      for (let i = 1; i < rows.length; i++) {
        const r = rows[i];
        if (!r[0]) continue;
        const rutNorm = normalizeRut(String(r[0]));
        if (!rutNorm) continue;
        if (checkCliente.get(rutNorm, empresaId)) { omitidosClientes++; continue; }
        insertCliente.run(
          empresaId, tipo, String(r[0]||''), rutNorm,
          String(r[1]||''), String(r[2]||''), String(r[3]||''),
          String(r[4]||''), String(r[5]||''),
          String(r[6]||r[1]||''),   // nombre = col 6 o razon social
          String(r[7]||''), String(r[8]||''),
          String(r[9]||''), String(r[10]||''),
          now, now
        );
        importadosClientes++;
      }
    })();
  }

  importarHojaClientes('DME RECIBIDAS', 'empresa');
  importarHojaClientes('RNE RECIBIDAS', 'empresa');
  importarHojaClientes('DMP RECIBIDAS', 'persona');
  importarHojaClientes('RNP RECIBIDAS', 'persona');

  // Importar hoja RECIBIDAS (movimientos históricos)
  const importarMov = req.body.importar_movimientos === 'true';
  if (importarMov && wb.SheetNames.includes('RECIBIDAS')) {
    const rows      = XLSX_LIB.utils.sheet_to_json(wb.Sheets['RECIBIDAS'], { header: 1, defval: '' });
    const checkDup  = db.prepare('SELECT id FROM movimientos WHERE id_compuesto=? AND empresa_id=?');
    const insertMov = db.prepare(`
      INSERT OR IGNORE INTO movimientos
        (empresa_id, id_transferencia, fecha, monto, rut, rut_normalizado,
         banco_origen, cuenta_origen, nombre_origen, banco_cartola, id_compuesto,
         estado, tipo_dte, razon_social, created_at, updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    db.transaction(() => {
      for (let i = 1; i < rows.length; i++) {
        const r = rows[i];
        if (!r[0]) continue;
        const idTransf    = String(r[0]).trim();
        const bancoCol    = String(r[10]||'').toUpperCase().trim() || 'IMPORTADO';
        const idCompuesto = `${empresaId}_${idTransf}_${bancoCol}`;
        if (checkDup.get(idCompuesto, empresaId)) { omitidosMov++; continue; }

        const rutEmp = String(r[2]||'').trim();
        const rutPer = String(r[3]||'').trim();
        const rut    = rutEmp || rutPer;
        const rutNorm= normalizeRut(rut);
        const tipoDte= rutEmp ? 34 : 41;

        let fecha = String(r[1]||'').split('T')[0];
        if (fecha.includes('/')) {
          const [d, m, y] = fecha.split('/');
          fecha = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
        }
        const monto = parseFloat(String(r[6]||'0').replace(/[.,]/g,'')) || 0;

        insertMov.run(
          empresaId, idTransf, fecha, monto,
          rut, rutNorm,
          String(r[4]||''), String(r[5]||''),
          String(r[7]||''), bancoCol, idCompuesto,
          'facturado', tipoDte, String(r[7]||''),
          now, now
        );
        importadosMov++;
      }
    })();
  }

  res.json({
    ok: true,
    clientes:    { importados: importadosClientes, omitidos: omitidosClientes },
    movimientos: { importados: importadosMov,      omitidos: omitidosMov }
  });
});

// ── Actualizar keys de SimpleAPI desde la UI (solo admin) ────────────────────
app.put('/api/simpleapi/keys', requireAuth, requireAdmin, (req, res) => {
  const { keys } = req.body;  // { 'tg-inversiones': '...', 'mt-inversiones': '...' }
  if (!keys || typeof keys !== 'object') return res.status(400).json({ error: 'Formato inválido' });
  setAppData('simpleapi_keys', keys);
  res.json({ ok: true });
});

app.get('/api/simpleapi/keys', requireAuth, requireAdmin, (req, res) => {
  const stored = getAppData('simpleapi_keys') || {};
  // Merge con defaults pero no revelar los valores completos si ya están
  const result = {};
  for (const empId of Object.keys(SIMPLEAPI_KEYS_DEFAULT)) {
    const key = stored[empId] || SIMPLEAPI_KEYS_DEFAULT[empId] || '';
    result[empId] = key ? key.substring(0,4) + '****' + key.slice(-4) : '';
  }
  res.json(result);
});

// ── Importar base histórica de DTE emitidos ───────────────────────────────────
// Acepta el Excel con hojas "FACTURAS INGRESADAS" y "BOLETAS INGRESADAS"
// Inserta cada fila como movimiento facturado, deduplica por id_compuesto
app.post('/api/importar/base-historica', requireAuth, upload.single('base'), (req, res) => {
  if (!XLSX_LIB)  return res.status(500).json({ error: 'Módulo xlsx no disponible' });
  if (!req.file)  return res.status(400).json({ error: 'No se subió archivo' });

  const empresaId = req.user.role === 'admin' ? (req.body.empresa_id || '') : req.user.empresa;
  if (!empresaId) return res.status(400).json({ error: 'Empresa no especificada' });

  let wb;
  try { wb = XLSX_LIB.read(req.file.buffer, { type: 'buffer' }); }
  catch(e) { return res.status(422).json({ error: 'No se pudo leer el archivo Excel: ' + e.message }); }

  const HOJAS_OBJETIVO = ['FACTURAS INGRESADAS', 'BOLETAS INGRESADAS'];
  const now = nowCL();

  // Normalizar el banco del id_compuesto para que coincida con el sistema (siempre MAYÚSCULAS)
  // Incluye empresa_id como prefijo para garantizar unicidad global entre empresas
  function normalizarIdCompuesto(idTransf, cartola) {
    const banco = String(cartola || '').trim().toUpperCase();
    const id    = String(idTransf || '').trim();
    return id && banco ? `${empresaId}_${id}_${banco}` : '';
  }

  // Determinar tipo por RUT: >= 50.000.000 → empresa (34) · < 50M → persona (41)
  function tipoPorRut(rutNorm) {
    const digits = rutNorm.replace(/[^0-9]/g, '');
    if (!digits) return 'empresa';
    return parseInt(digits.slice(0, -1)) >= 50000000 ? 'empresa' : 'persona';
  }

  // Leer id_compuesto existentes en esta empresa (para deduplicar en memoria)
  const existentesSet = new Set(
    db.prepare("SELECT id_compuesto FROM movimientos WHERE empresa_id = ? AND id_compuesto IS NOT NULL AND id_compuesto != ''")
      .all(empresaId).map(r => r.id_compuesto.toUpperCase())
  );

  // Crear un lote registral para esta importación
  const loteId   = generateLoteId(empresaId);
  const nombreLote = `Base histórica ${empresaId}`;

  const stmtInsertMov = db.prepare(`
    INSERT OR IGNORE INTO movimientos
      (empresa_id, id_transferencia, fecha, monto, monto_total, glosa,
       rut, rut_normalizado, nombre_origen, razon_social, giro,
       direccion, comuna, ciudad, email_receptor,
       banco_cartola, banco_origen, id_compuesto,
       estado, tipo_dte, fecha_facturacion, lote_id,
       nombre_item, descripcion_item, precio,
       fecha_carga, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const stmtInsertCliente = db.prepare(`
    INSERT OR IGNORE INTO clientes
      (empresa_id, tipo, rut, rut_normalizado, razon_social, giro,
       direccion, comuna, ciudad, email, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  let insertados = 0, duplicados = 0, reconciliados = 0, clientesNuevos = 0, errores = 0;
  const nuevosIdCompuesto = new Set(); // para deduplicar dentro del propio Excel

  const stmtGetExistente = db.prepare(
    "SELECT id, estado FROM movimientos WHERE id_compuesto = ? AND empresa_id = ?"
  );
  const stmtReconciliar = db.prepare(
    `UPDATE movimientos SET estado='facturado', lote_id=?, fecha_facturacion=?, updated_at=?
     WHERE id=? AND estado IN ('listo','pendiente')`
  );

  const procesarHoja = (sheetName) => {
    if (!wb.SheetNames.includes(sheetName)) return;
    const ws   = wb.Sheets[sheetName];
    const rows = XLSX_LIB.utils.sheet_to_json(ws, { defval: '' });

    for (const row of rows) {
      try {
        const idTransf   = String(row['ID Transferencia'] || '').trim();
        const cartola    = String(row['Cartola'] || '').trim();
        const idComp     = normalizarIdCompuesto(idTransf, cartola);

        if (!idComp) { errores++; continue; }

        const idCompUpper = idComp.toUpperCase();

        // Deduplicar dentro del propio archivo
        if (nuevosIdCompuesto.has(idCompUpper)) { duplicados++; continue; }

        // Ya existe en la BD
        if (existentesSet.has(idCompUpper)) {
          // Reconciliar: si está listo/pendiente → marcar como facturado
          const existente = stmtGetExistente.get(idComp, empresaId);
          if (existente && (existente.estado === 'listo' || existente.estado === 'pendiente')) {
            // Intentar leer fecha del Excel para la facturación
            let fechaReconcil = now;
            const fechaRawR = row['FechaEmision'] || row['Fecha de emisión (*)'] || row['Fecha de emisión'];
            if (fechaRawR instanceof Date) {
              const dd = String(fechaRawR.getDate()).padStart(2,'0');
              const mm = String(fechaRawR.getMonth()+1).padStart(2,'0');
              fechaReconcil = `${dd}/${mm}/${fechaRawR.getFullYear()}`;
            } else if (typeof fechaRawR === 'number') {
              const d = XLSX_LIB.SSF.parse_date_code(fechaRawR);
              if (d) fechaReconcil = `${String(d.d).padStart(2,'0')}/${String(d.m).padStart(2,'0')}/${d.y}`;
            } else if (fechaRawR) {
              fechaReconcil = String(fechaRawR).substring(0, 10);
            }
            const r = stmtReconciliar.run(loteId, fechaReconcil, now, existente.id);
            if (r.changes > 0) reconciliados++;
            else duplicados++;
          } else {
            duplicados++;
          }
          continue;
        }

        // Fecha de emisión (SF CSV: 'FechaEmision' | TS Capital Excel: 'Fecha de emisión (*)')
        let fechaStr = '';
        const fechaRaw = row['FechaEmision'] || row['Fecha de emisión (*)'] || row['Fecha de emisión'];
        if (fechaRaw instanceof Date) {
          const dd = String(fechaRaw.getDate()).padStart(2,'0');
          const mm = String(fechaRaw.getMonth()+1).padStart(2,'0');
          fechaStr = `${dd}/${mm}/${fechaRaw.getFullYear()}`;
        } else if (typeof fechaRaw === 'number') {
          // Excel serial date
          const d = XLSX_LIB.SSF.parse_date_code(fechaRaw);
          if (d) fechaStr = `${String(d.d).padStart(2,'0')}/${String(d.m).padStart(2,'0')}/${d.y}`;
        } else if (fechaRaw) {
          fechaStr = String(fechaRaw).substring(0, 10);
        }

        // Soporte multi-formato: SF CSV ('RutRecep', 'RazonSocialRecep'…)
        //                     y TS Capital Excel ('RUT Receptor', 'Razón Social'…)
        const rutRaw   = String(row['RutRecep'] || row['RUT Receptor'] || '').trim();
        const rutNorm  = normalizeRut(rutRaw);
        const rutFmt   = rutNorm ? formatRut(rutNorm) : '';
        const razon    = String(row['RazonSocialRecep'] || row['Razón Social'] || row['Contacto'] || '').trim();
        const giro     = String(row['GiroRecep']  || row['Giro'] || '').trim();
        const dir      = String(row['DirRecep']   || row['Dirección'] || '').trim();
        const comuna   = String(row['CmnaRecep']  || row['Comuna'] || '').trim();
        const ciudad   = String(row['CiudadRecep']|| row['Ciudad'] || '').trim();
        const email    = String(row['CorreoRecep']|| row['Email Receptor'] || '').trim();
        const monto    = parseFloat(row['TotalProducto'] || row['Monto total'] || row['Precio (*)'] || row['Precio (*) '] || 0) || 0;
        // Columna '1' contiene '34: Factura exenta…' o '41: Boleta exenta…'
        const tipoRaw  = String(row['TipoDte'] || row['1'] || '').trim();
        const tipoDte  = tipoRaw ? (parseInt(tipoRaw) || (tipoRaw.startsWith('41') ? 41 : 34)) : 34;
        const tipoCliente = tipoPorRut(rutNorm);
        const bancoUp  = cartola.toUpperCase();
        const descProd = String(row['DescripcionProducto'] || '').trim();
        const nombreProd = String(row['NombreProducto'] || '').trim();
        const precio   = parseFloat(row['PrecioProducto']) || 0;

        stmtInsertMov.run(
          empresaId, idTransf, fechaStr, monto, monto, descProd || nombreProd,
          rutFmt, rutNorm, razon, razon, giro,
          dir, comuna, ciudad, email,
          bancoUp, bancoUp, idComp,
          'facturado', tipoDte, fechaStr, loteId,
          nombreProd, descProd, precio,
          now, now, now
        );

        // Insertar/ignorar cliente
        if (rutNorm) {
          const antes = db.prepare('SELECT id FROM clientes WHERE rut_normalizado = ? AND empresa_id = ?').get(rutNorm, empresaId);
          if (!antes) {
            stmtInsertCliente.run(
              empresaId, tipoCliente, rutFmt, rutNorm, razon || null, giro || null,
              dir || null, comuna || null, ciudad || null, email || null, now, now
            );
            clientesNuevos++;
          }
        }

        nuevosIdCompuesto.add(idCompUpper);
        insertados++;
      } catch(rowErr) {
        errores++;
      }
    }
  };

  try {
    db.transaction(() => {
      // Crear lote registral antes de insertar movimientos
      db.prepare(
        'INSERT OR IGNORE INTO lotes_facturacion (lote_id, empresa_id, nombre, cantidad, monto_total, estado, metodo, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)'
      ).run(loteId, empresaId, nombreLote, 0, 0, 'facturado_manual', 'importacion_historica', now, now);

      // Procesar todas las hojas
      for (const hoja of HOJAS_OBJETIVO) procesarHoja(hoja);

      // Actualizar totales reales del lote tras procesar filas
      const totalesLote = db.prepare(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(monto_total),0) as monto FROM movimientos WHERE lote_id = ? AND empresa_id = ?"
      ).get(loteId, empresaId);
      db.prepare('UPDATE lotes_facturacion SET cantidad = ?, monto_total = ?, updated_at = ? WHERE lote_id = ?')
        .run(totalesLote.cnt, totalesLote.monto, now, loteId);
    })();

    res.json({
      ok: true,
      insertados,
      reconciliados,
      duplicados,
      clientesNuevos,
      errores,
      lote_id: loteId,
      nombre: nombreLote
    });
  } catch(e) {
    console.error('[BASE HISTORICA]', e);
    res.status(500).json({ error: 'Error procesando base histórica: ' + e.message });
  }
});

app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));