require("dotenv").config({ path: ".env.import" });

const XLSX = require("xlsx");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("Falta SUPABASE_URL o SUPABASE_SERVICE_ROLE en .env.import");
  process.exit(1);
}

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

const FILE = process.argv[2] || "COBRO DE ALQUILERS 2026.xlsx";
const YEAR = Number(process.argv[3] || 2026);
const DEFAULT_DIA_VENC = 10;

const MONTHS = {
  enero: 1, febrero: 2, marzo: 3, abril: 4, mayo: 5, junio: 6,
  julio: 7, agosto: 8, septiembre: 9, octubre: 10, noviembre: 11, diciembre: 12,
};

const norm = (v) => String(v ?? "").trim();
const isEmpty = (v) => v === null || v === undefined || String(v).trim() === "";

function inferTipoFromCodigo(codigo) {
  const s = codigo.toUpperCase();
  if (s.startsWith("PC")) return "PATIO DE COMIDA";
  if (s.startsWith("P")) return "PUESTO";
  if (s.startsWith("L")) return "LOCAL";
  if (s.startsWith("I")) return "ISLA";
  if (s === "KIOSKO") return "KIOSKO";
  if (s === "LAVADERO") return "LAVADERO";
  return "OTRO";
}

function toMoney(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;

  let s = String(v).trim().replace(/[^\d,.\-]/g, "");
  const hasComma = s.includes(",");
  const hasDot = s.includes(".");

  if (hasComma && hasDot) {
    if (s.lastIndexOf(",") > s.lastIndexOf(".")) s = s.replace(/\./g, "").replace(",", ".");
    else s = s.replace(/,/g, "");
  } else if (hasComma && !hasDot) {
    s = s.replace(",", ".");
  } else {
    // miles tipo 1.200.000
    if ((s.match(/\./g) || []).length >= 2) s = s.replace(/\./g, "");
    // caso 70.000
    const m = s.match(/^(\-?\d+)\.(\d{3})$/);
    if (m) s = m[1] + m[2];
  }

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function lastDayOfMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

function dateStr(y, m, d) {
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

function findRow(rows, predicate) {
  for (let i = 0; i < rows.length; i++) if (predicate(rows[i], i)) return i;
  return -1;
}

function buildReceiptsMap(rows) {
  // busca "NRO DE RECIBOS"
  const recStart = findRow(rows, (r) => norm(r?.[0]).toUpperCase().includes("NRO DE RECIBOS"));
  if (recStart === -1) return {};

  // fila siguiente tiene "RECIBO 1" en col 2
  const hdr = recStart + 1;
  const dataStart = hdr + 1;
  const map = {};

  for (let i = dataStart; i < rows.length; i++) {
    const mName = norm(rows[i]?.[0]).toLowerCase();
    if (!MONTHS[mName]) break;

    map[mName] = [];
    for (let k = 0; k < 8; k++) {
      const val = rows[i][2 + k]; // col C..J
      map[mName][k] = isEmpty(val) ? null : String(val).trim();
    }
  }
  return map;
}

async function getOrCreateInquilino(nombre) {
  const { data, error } = await sb.from("inquilinos").select("id,nombre").eq("nombre", nombre).limit(1).maybeSingle();
  if (error) throw error;
  if (data?.id) return data.id;

  const ins = await sb.from("inquilinos").insert({ nombre }).select("id").single();
  if (ins.error) throw ins.error;
  return ins.data.id;
}

async function getLocalIdByCodigo(codigo) {
  const { data, error } = await sb.from("locales").select("id,codigo").eq("codigo", codigo).limit(1).maybeSingle();
  if (error) throw error;
  return data?.id || null;
}

async function upsertLocal({ codigo, tipo, descripcion }) {
  const payload = { codigo, tipo, descripcion: descripcion || null, estado: "OCUPADO" };
  const { error } = await sb.from("locales").upsert(payload, { onConflict: "codigo" });
  if (error) throw error;
  return await getLocalIdByCodigo(codigo);
}

async function getOrCreateContrato({ localId, inqId, fechaInicio, alquilerInicial }) {
  // contrato activo por local
  const q = await sb
    .from("contratos")
    .select("id, dia_vencimiento, inquilino_id")
    .eq("local_id", localId)
    .eq("estado", "ACTIVO")
    .limit(1)
    .maybeSingle();
  if (q.error) throw q.error;

  if (!q.data) {
    const ins = await sb
      .from("contratos")
      .insert({
        local_id: localId,
        inquilino_id: inqId,
        fecha_inicio: fechaInicio,
        fecha_fin: null,
        alquiler_inicial: Number(alquilerInicial).toFixed(2),
        deposito: "0.00",
        dia_vencimiento: DEFAULT_DIA_VENC,
        estado: "ACTIVO",
      })
      .select("id, dia_vencimiento, inquilino_id")
      .single();
    if (ins.error) throw ins.error;
    return ins.data;
  }

  // Si estaba SIN ASIGNAR o distinto, lo actualizamos al nombre real del COBRO
  if (q.data.inquilino_id !== inqId) {
    await sb.from("contratos").update({ inquilino_id: inqId }).eq("id", q.data.id);
  }

  return q.data;
}

async function upsertAjustesPorCosto(contratoId, costoPorMes) {
  // costoPorMes: array 12 con números o 0
  let prev = null;
  const ajustes = [];
  for (let m = 1; m <= 12; m++) {
    const val = costoPorMes[m - 1];
    if (!val || val <= 0) continue;
    if (prev === null) prev = val;
    if (val !== prev) {
      ajustes.push({
        contrato_id: contratoId,
        vigente_desde: dateStr(YEAR, m, 1),
        tipo: "MONTO",
        valor: Number(val).toFixed(2),
        nota: "Import COBRO 2026",
      });
      prev = val;
    }
  }
  if (!ajustes.length) return;

  const { error } = await sb
    .from("contrato_ajustes")
    .upsert(ajustes, { onConflict: "contrato_id,vigente_desde" });
  if (error) throw error;
}

async function upsertCuota({ contratoId, mes, diaVenc, total }) {
  const vencDay = Math.min(diaVenc, lastDayOfMonth(YEAR, mes));
  const payload = {
    contrato_id: contratoId,
    periodo: dateStr(YEAR, mes, 1),
    vencimiento: dateStr(YEAR, mes, vencDay),
    alquiler_mes: Number(total).toFixed(2),
    total: Number(total).toFixed(2),
  };
  const { error } = await sb.from("cuotas").upsert(payload, { onConflict: "contrato_id,periodo" });
  if (error) throw error;

  // traer id de cuota
  const { data, error: qErr } = await sb
    .from("cuotas")
    .select("id, vencimiento")
    .eq("contrato_id", contratoId)
    .eq("periodo", payload.periodo)
    .limit(1)
    .single();
  if (qErr) throw qErr;
  return data;
}

async function existsPago(contratoId, referencia) {
  const { data, error } = await sb
    .from("pagos")
    .select("id")
    .eq("contrato_id", contratoId)
    .eq("referencia", referencia)
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return !!data?.id;
}

async function insertPagoAplicado({ contratoId, cuotaId, fechaPago, monto, referencia }) {
  // dedupe por referencia
  if (await existsPago(contratoId, referencia)) return;

  const insPago = await sb
    .from("pagos")
    .insert({
      contrato_id: contratoId,
      fecha_pago: fechaPago,
      monto: Number(monto).toFixed(2),
      medio: "IMPORT_COBRO",
      referencia,
    })
    .select("id")
    .single();
  if (insPago.error) throw insPago.error;

  const pagoId = insPago.data.id;

  const insAp = await sb.from("pago_aplicaciones").insert({
    pago_id: pagoId,
    cuota_id: cuotaId,
    monto: Number(monto).toFixed(2),
  });
  if (insAp.error) throw insAp.error;
}

(async () => {
  const wb = XLSX.readFile(FILE, { cellDates: true });
  const sheets = wb.SheetNames;

  const skipSheets = new Set(["INTERESES", "MATRIZ"]);
  let ok = 0, skipped = 0;

  for (const sheetName of sheets) {
    const sName = sheetName.trim();
    if (skipSheets.has(sName.toUpperCase())) continue;

    const ws = wb.Sheets[sName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    // Debe tener tabla "PAGOS DEL ALQUILER"
    const pagosRow = findRow(rows, (r) => norm(r?.[0]).toUpperCase().includes("PAGOS DEL ALQUILER"));
    if (pagosRow === -1) continue;

    const codigo = sName.toUpperCase();
    const descripcion = norm(rows?.[0]?.[0]) || null;
    const inquilinoNombre = norm(rows?.[0]?.[2]); // C1

    if (!inquilinoNombre) { skipped++; continue; }

    const tipo = inferTipoFromCodigo(codigo);

    // Data de meses
    const startData = pagosRow + 2;
    const receiptsMap = buildReceiptsMap(rows);

    // costo por mes para ajustes
    const costoMes = Array(12).fill(0);

    // buscar primer mes con costo > 0 para fecha_inicio y alquiler_inicial
    let firstMonth = null;
    let firstCost = null;

    // 1) Upsert local + inquilino + contrato
    const localId = await upsertLocal({ codigo, tipo, descripcion });
    const inqId = await getOrCreateInquilino(inquilinoNombre);

    // leer costos
    for (let i = startData; i < rows.length; i++) {
      const mName = norm(rows[i]?.[0]).toLowerCase();
      if (!MONTHS[mName]) break;
      const mes = MONTHS[mName];
      const cost = toMoney(rows[i]?.[1]);
      if (cost && cost > 0) {
        costoMes[mes - 1] = cost;
        if (firstMonth === null) { firstMonth = mes; firstCost = cost; }
      }
    }

    if (!firstMonth || !firstCost) { skipped++; continue; }

    const fechaInicio = dateStr(YEAR, firstMonth, 1);
    const contrato = await getOrCreateContrato({
      localId,
      inqId,
      fechaInicio,
      alquilerInicial: firstCost,
    });

    const contratoId = contrato.id;
    const diaVenc = contrato.dia_vencimiento || DEFAULT_DIA_VENC;

    // ajustes por cambios de costo
    await upsertAjustesPorCosto(contratoId, costoMes);

    // 2) cuotas + pagos (directos al mes)
    for (let i = startData; i < rows.length; i++) {
      const mName = norm(rows[i]?.[0]).toLowerCase();
      if (!MONTHS[mName]) break;

      const mes = MONTHS[mName];
      const cost = toMoney(rows[i]?.[1]);
      if (!cost || cost <= 0) continue;

      const cuota = await upsertCuota({ contratoId, mes, diaVenc, total: cost });
      const cuotaId = cuota.id;
      const fechaPagoDefault = String(cuota.vencimiento).slice(0, 10);

      // pagos 1..8 en cols C..J
      for (let k = 0; k < 8; k++) {
        const p = toMoney(rows[i]?.[2 + k]);
        if (!p || p <= 0) continue;

        const recibo = receiptsMap[mName]?.[k] || null;
        const referencia = recibo
          ? `RECIBO:${recibo}`
          : `COBRO:${codigo}:${dateStr(YEAR, mes, 1)}:P${k + 1}`;

        await insertPagoAplicado({
          contratoId,
          cuotaId,
          fechaPago: fechaPagoDefault,
          monto: p,
          referencia,
        });
      }
    }

    ok++;
    if (ok % 25 === 0) console.log(`Importados ${ok}...`);
  }

  console.log(`✅ Import COBRO finalizado. Hojas procesadas: ${ok}. Saltadas: ${skipped}`);
  process.exit(0);
})().catch((e) => {
  console.error("❌ Error:", e.message || e);
  process.exit(1);
});