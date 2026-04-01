# MEMORIA DE DESARROLLO — Sistema de Facturación MT TG
> Documento vivo de patrones, errores resueltos e iteraciones del proyecto.
> Uso: referencia para construir portales similares más rápido y sin repetir errores.

---

## 1. STACK Y ARQUITECTURA

```
Node.js + Express
better-sqlite3 (SQLite sincrónico, NO async)
JWT (4h expiry) + SHA-256 para passwords
Vanilla JS SPA (sin frameworks)
multer (memory storage) para uploads
xlsx (v0.18.5) para parsing server-side de Excel
Railway (deploy automático desde GitHub)
SQLite persistido en /data (volumen Railway)
```

### Estructura de archivos
```
/
├── server.js          — Toda la lógica backend
├── public/
│   └── index.html     — SPA completo (HTML + CSS + JS inline)
├── data/
│   └── facturacion.db — SQLite (solo en Railway, no commitear)
└── package.json
```

### Regla de oro del stack
- **`better-sqlite3` es sincrónico** — no usar `await` con él.
- **SHA-256 para passwords** — no bcrypt (evita dependencias nativas en Railway).
- **JWT secret** en variable de entorno `JWT_SECRET`.
- **SQLite en Railway** — `const DB_PATH = process.env.DB_PATH || '/data/facturacion.db'`. El volumen `/data` es persistente.

---

## 2. ESQUEMA BASE DE DATOS

### Tabla `movimientos` (tabla central)
```sql
CREATE TABLE IF NOT EXISTS movimientos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  empresa_id TEXT,
  id_transferencia TEXT,
  fecha TEXT,
  monto REAL,
  monto_total REAL,
  glosa TEXT,
  rut TEXT,                   -- Formateado con puntos y guión
  rut_normalizado TEXT,       -- Solo dígitos + DV sin puntos
  nombre_origen TEXT,
  razon_social TEXT,
  giro TEXT,
  direccion TEXT,
  comuna TEXT,
  ciudad TEXT,
  email_receptor TEXT,
  banco_origen TEXT,
  banco_cartola TEXT,         -- SIEMPRE MAYÚSCULAS: 'SANTANDER' | 'BCI'
  cuenta_origen TEXT,
  id_compuesto TEXT UNIQUE,   -- Clave de deduplicación: {id_transferencia}_{BANCO}
  estado TEXT,                -- Ver máquina de estados abajo
  tipo_dte INTEGER,           -- 34 = Factura Exenta, 41 = Boleta
  lote_id TEXT,
  lote_carga_id TEXT,         -- ID del batch de carga de cartola
  fecha_facturacion TEXT,
  fecha_carga TEXT,
  cargado_por TEXT,           -- username que subió la cartola
  nombre_item TEXT,
  descripcion_item TEXT,
  precio REAL,
  monto_exento REAL,
  created_at TEXT,
  updated_at TEXT
)
```

### Tabla `lotes_facturacion`
```sql
CREATE TABLE IF NOT EXISTS lotes_facturacion (
  lote_id TEXT PRIMARY KEY,
  empresa_id TEXT,
  nombre TEXT,                -- "Lote 31-03-2026 100 DTE"
  cantidad INTEGER,
  monto_total REAL,
  estado TEXT,
  metodo TEXT,                -- 'api' | 'manual' | 'importacion_historica'
  created_at TEXT,
  updated_at TEXT
)
```

---

## 3. MÁQUINA DE ESTADOS DE MOVIMIENTOS

```
[cartola nueva] → pendiente → listo → en_lote → facturado
                           ↘ facturado  (marcar manual)
[RUT propio]    → interno   (nunca aparece en Facturar)
```

| Estado | Descripción |
|--------|-------------|
| `pendiente` | Sin datos suficientes. Aparece en Movimientos |
| `listo` | Cliente conocido, listo para facturar |
| `en_lote` | Incluido en lote de facturación en proceso |
| `facturado` | Emitida o marcada manualmente |
| `interno` | Transferencia entre empresas propias — NUNCA facturar |

### Cleanup de errores al iniciar (crítico)
```javascript
db.exec(`UPDATE movimientos SET estado='listo', lote_id=NULL WHERE
         lote_id IN (SELECT lote_id FROM lotes_facturacion WHERE estado='error')`);
db.exec(`DELETE FROM lotes_facturacion WHERE estado='error'`);
```

---

## 4. MANEJO DE RUT

```javascript
function normalizeRut(rut) {
  return String(rut || '').replace(/[.\-\s]/g, '').toUpperCase();
}
// '77.859.376-9' → '778593769'

// Búsqueda frontend ignorando puntos:
const qRut = q.replace(/\./g, '');
const rut  = (m.rut || '').replace(/\./g, '');
return rut.includes(qRut);
```

---

## 5. ID COMPUESTO — CLAVE DE DEDUPLICACIÓN

```
id_compuesto = {id_transferencia}_{BANCO}
Ej: "21044_SANTANDER", "98765_BCI"
```

**El banco SIEMPRE en MAYÚSCULAS.** El Excel histórico puede traer "Santander" — normalizar con `.toUpperCase()` antes de cualquier comparación.

```javascript
function normalizarIdCompuesto(idTransf, cartola) {
  const banco = String(cartola || '').trim().toUpperCase();
  const id    = String(idTransf || '').trim();
  return id && banco ? `${id}_${banco}` : '';
}
```

---

## 6. RECONCILIACIÓN BASE HISTÓRICA (PROBLEMA FRECUENTE)

### El problema
Si cartolas se cargan ANTES de importar la base histórica:
1. Cartola → movimiento X con `estado='listo'`
2. Import histórico → X detectado como duplicado → **saltado**
3. X permanece `listo` aunque ya esté emitido → aparece en Facturar

### La solución
Al encontrar un duplicado en el import, verificar su estado actual:
```javascript
if (existentesSet.has(idCompUpper)) {
  const existente = db.prepare(
    "SELECT id, estado FROM movimientos WHERE id_compuesto = ? AND empresa_id = ?"
  ).get(idComp, empresaId);

  if (existente && ['listo','pendiente'].includes(existente.estado)) {
    db.prepare(`UPDATE movimientos SET estado='facturado', lote_id=?,
                fecha_facturacion=?, updated_at=? WHERE id=?`)
      .run(loteId, fechaEmision, now, existente.id);
    reconciliados++;
  } else { duplicados++; }
  continue;
}
```

Respuesta: `{ insertados, reconciliados, duplicados, errores }`

---

## 7. RUTS INTERNOS (EXCLUIR DE FACTURAR)

```javascript
const RUTS_INTERNOS = ['778593769', '778856980']; // TG / MT Inversiones

// Al procesar cartola:
if (RUTS_INTERNOS.includes(rutNorm)) { estado = 'interno'; }

// Migration al iniciar:
db.exec(`UPDATE movimientos SET estado='interno' WHERE
         rut_normalizado IN ('778593769','778856980')
         AND estado IN ('pendiente','listo')`);
```

---

## 8. AUTENTICACIÓN — ERRORES CRÍTICOS

### Clave localStorage incorrecta
```javascript
// ❌ MAL — causa 401 silencioso en operadores
localStorage.getItem('token')

// ✅ CORRECTO
localStorage.getItem('mttg_token')  // Definir UNA sola constante al inicio
```

Hay 4+ lugares con fetch manual (upload cartola, export, import). Todos deben usar la misma clave. Mejor usar un helper:
```javascript
async function apiFetch(url, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...opts.headers };
  if (TOKEN) headers.Authorization = `Bearer ${TOKEN}`;
  const res = await fetch(url, { ...opts, headers });
  if (res.status === 401) { doLogout(); return null; }
  return res.json();
}
```

---

## 9. PAGINACIÓN SERVER-SIDE CON ORDEN

```javascript
// Server: whitelist de columnas (anti SQL injection)
const COLS = { fecha: 'fecha', monto: 'monto', rut: 'rut', razon_social: 'razon_social' };
const sortCol = COLS[req.query.orden] || 'fecha';
const sortDir = req.query.dir === 'desc' ? 'DESC' : 'ASC';

if (req.query.pag === '1') {
  const total = db.prepare(countSql).get(...params)?.total || 0;
  sql += ` ORDER BY ${sortCol} ${sortDir}, id ${sortDir}`;
  sql += ' LIMIT ? OFFSET ?'; params.push(limit, offset);
  return res.json({ movimientos: db.prepare(sql).all(...params), total });
}
// Legado: array directo
res.json(db.prepare(sql).all(...params));
```

---

## 10. PERSISTENCIA DE VISTA EN F5

```javascript
function adminNav(view) {
  CURRENT_VIEW = view;
  localStorage.setItem('mttg_view', view); // ← persistir
  // ... activar el panel correspondiente
}

function showAdmin() {
  const lastView = localStorage.getItem('mttg_view') || 'dashboard';
  const valid = ['dashboard','movimientos','facturacion',...];
  adminNav(valid.includes(lastView) ? lastView : 'dashboard');
}

function doLogout() {
  localStorage.removeItem('mttg_token');
  localStorage.removeItem('mttg_view'); // ← limpiar al salir
}
```

---

## 11. SCHEMA MIGRATIONS IDEMPOTENTES

```javascript
// NUNCA fallan si la columna ya existe
try { db.exec('ALTER TABLE movimientos ADD COLUMN cargado_por TEXT'); } catch(e){}
try { db.exec('ALTER TABLE lotes_facturacion ADD COLUMN nombre TEXT'); } catch(e){}
```

---

## 12. RESTRICCIONES POR ROL

| Feature | Admin | Operador |
|---------|-------|----------|
| Montos en dashboard | ✅ | ❌ |
| Movimientos histórico completo | ✅ | ❌ (últimos 45 días) |
| Base Histórica DTE | ✅ | ❌ |
| Facturar / Lotes / Exportar | ✅ | ✅ |

```javascript
// 45 días para operadores
if (!isAdmin()) {
  const d = new Date(); d.setDate(d.getDate() - 45);
  url += `&fecha_desde=${d.toISOString().slice(0, 10)}`;
}

// Ocultar montos
const mostrarMontos = isAdmin();
${mostrarMontos ? `<div>Monto total...</div>` : ''}
```

---

## 13. HORA CHILE EN SERVER

```javascript
function nowCL() {
  return new Date().toLocaleString('sv-SE', { timeZone: 'America/Santiago' })
    .replace(' ', 'T');
}
// "2026-03-31T14:35:22"
```

---

## 14. NOMBRE DE LOTE

```
"Lote {fecha_ultimo_dte} {N} DTE"
"Lote 31-03-2026 100 DTE"
"Marcado facturado manual"
"Base histórica {empresa_id}"
```

La fecha se obtiene con `maxFechaLote(movs)` — parsea DD/MM/YYYY y YYYY-MM-DD, devuelve DD-MM-YYYY del más reciente.

---

## 15. TABLA DE ERRORES FRECUENTES

| Error observable | Causa raíz | Solución |
|------------------|-----------|----------|
| "Token inválido" en operadores | `localStorage.getItem('token')` incorrecto | Usar clave `mttg_token` en TODOS los fetch |
| Facturas ya emitidas en Facturar | Import histórico saltó duplicados listo | Re-subir Excel con reconciliación activa |
| Movimientos sin orden cronológico | `ORDER BY id DESC` ignora fecha real | `ORDER BY fecha ASC, id ASC` |
| Banco lowercase en id_compuesto | Excel trae "Santander" sin normalizar | `.toUpperCase()` siempre al construir id_compuesto |
| F5 vuelve al inicio | Vista no persistida | `localStorage.setItem('mttg_view', view)` |
| Lotes con error bloquean movimientos | Estado `en_lote` sin lote válido | Cleanup al iniciar server |
| Transferencias propias en Facturar | RUTs no excluidos | Array `RUTS_INTERNOS` + estado `interno` |
| Montos visibles para operadores | Sin condicional de rol en dashboard | `const mostrarMontos = isAdmin()` |

---

## 16. FLUJO COMPLETO DEL NEGOCIO

```
Operador sube cartola (XLS/XLSX bancario)
      ↓
Frontend (SheetJS) parsea → envía a /api/movimientos/procesar
      ↓
Server deduplica por id_compuesto — registra cargado_por
      ↓ si nuevo
Busca cliente en BD → si existe: estado='listo' | si no: 'pendiente'
Si RUT interno → estado='interno' (invisible siempre)
      ↓
Sección "Facturar" — opciones:
  A) Crear lote → nombre "Lote {fecha} {N} DTE" → API SimpleFactura → facturado
  B) Marcar manual → "Marcado facturado manual" → Historial
  C) Quitar de lista → estado='pendiente' (vuelve a Movimientos)
      ↓
Historial: todos los facturados con nombre de lote y fecha
```

---

## 17. CHECKLIST PARA PROYECTOS SIMILARES

- [ ] Definir `id_compuesto` para deduplicación desde el día 1
- [ ] Definir la máquina de estados antes de construir
- [ ] Normalizar RUT y banco en TODOS los puntos de entrada
- [ ] Una sola clave de `localStorage` para el token — helper `apiFetch()`
- [ ] Cleanup de errores al inicio del server (idempotente)
- [ ] `cargado_por` desde el inicio (trazabilidad)
- [ ] Migraciones con `try/catch`
- [ ] Whitelist de columnas en ORDER BY
- [ ] `mttg_view` persistido en navegación, limpiado en logout
- [ ] Variables de entorno Railway definidas antes del deploy
- [ ] Reconciliación en import histórico (no solo skip de duplicados)
- [ ] RUTs internos excluidos desde el primer procesamiento
