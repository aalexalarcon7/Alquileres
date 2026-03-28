// import_balance_2026_supabase.js
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

const FILE = process.argv[2] || "Balance 2026.xlsx";
const YEAR = Number(process.argv[3] || 2026);
const SHEET = "DEUDAS, RECAUDADO Y PROYECTADO";
const DEFAULT_DIA_VENC = 10;

const conceptToCode = (concept, nro) => {
  const n = String(Number(nro)).padStart(3, "0");
  const c = String(concept || "").trim().toLowerCase();

  if (c === "puestos") return `P${n}`;
  if (c === "local") return `L${n}`;
  if (c === "isla") return `I${n}`;
  if (c === "patio de comida") return `PC${n}`;
  if (c === "kiosko") return `KIOSKO`;       // en tu otro Excel existe así
  if (c === "lavadero") return `LAVADERO`;   // existe así
  if (c === "remis") return `REMIS`;         // no lo vi en el otro, pero lo dejamos
  return `${concept}-${n}`;
};

const norm = (v) => String(v ?? "").trim();
const isEmpty = (v) => v === null || v === undefined || String(v).trim() === "";

const lastDay = (y, m) => new Date(y, m, 0).getDate(); // m: 1-12
const dateStr = (y, m, d) =>
  `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;

const toMoney = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/[^\d,.\-]/g, "");
  const n = Number(s.replace(/\./g, "").replace(",", "."));
  return Number.isFinite(n) ? n : null;
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

async function main() {
  const wb = XLSX.readFile(FILE, { cellDates: true });
  const ws = wb.Sheets[SHEET];
  if (!ws) {
    console.error(`No existe la hoja "${SHEET}" en ${FILE}`);
    process.exit(1);
  }

  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  // En tu hoja:
  // SALDO: A..P (1..16) -> concept/nro/locatario en cols 1..3
  // RECAUDADO: R..AG (18..33) -> meses en cols 21..32
  // PROYECTADO: AI..AX (35..50) -> meses en cols 38..49
  // 0-based indices:
  const SALDO_CONCEPT = 0, SALDO_NRO = 1, SALDO_LOC = 2;
  const RECAUDADO_LOC = 19, RECAUDADO_JAN = 20; // 20..31
  const PROYECTADO_LOC = 36, PROYECTADO_JAN = 37; // 37..48

  const items = [];
  for (let i = 3; i < rows.length; i++) { // arranca en fila 4
    const concept = rows[i][SALDO_CONCEPT];
    const nro = rows[i][SALDO_NRO];

    if (typeof concept !== "string") continue;
    const conceptClean = concept.trim();
    if (!conceptClean || conceptClean.toUpperCase() === "TOTAL MES") continue;
    if (nro === null || nro === undefined || nro === "") continue;

    // nombre puede aparecer en cualquiera de los 3 bloques
    const locName =
      (!isEmpty(rows[i][SALDO_LOC]) ? rows[i][SALDO_LOC] :
      (!isEmpty(rows[i][RECAUDADO_LOC]) ? rows[i][RECAUDADO_LOC] :
      rows[i][PROYECTADO_LOC]));

    const codigo = conceptToCode(conceptClean, nro);
    const inquilinoNombre = !isEmpty(locName) ? norm(locName) : `SIN ASIGNAR - ${codigo}`;

    // meses PROYECTADO
    const proyectado = [];
    for (let m = 1; m <= 12; m++) {
      const v = toMoney(rows[i][PROYECTADO_JAN + (m - 1)]) ?? 0;
      proyectado.push(v);
    }

    // meses RECAUDADO
    const recaudado = [];
    for (let m = 1; m <= 12; m++) {
      const v = toMoney(rows[i][RECAUDADO_JAN + (m - 1)]) ?? 0;
      recaudado.push(v);
    }

    items.push({ concept: conceptClean, nro: Number(nro), codigo, inquilinoNombre, proyectado, recaudado });
  }

  console.log(`Filas detectadas para importar: ${items.length}`);

  // -------- 1) UPSERT LOCALES (batch)
  const localesPayload = Array.from(
    new Map(
      items.map((x) => [
      x.codigo,
      { codigo: x.codigo, tipo: x.concept, estado: "OCUPADO" },
    ])
  ).values()
);

  for (const part of chunk(localesPayload, 500)) {
    const { error } = await sb.from("locales").upsert(part, { onConflict: "codigo" });
    if (error) throw error;
  }

  // Traer ids de locales
  const allCodigos = [...new Set(items.map((x) => x.codigo))];
  const localesMap = new Map();
  for (const part of chunk(allCodigos, 500)) {
    const { data, error } = await sb.from("locales").select("id,codigo").in("codigo", part);
    if (error) throw error;
    data.forEach((r) => localesMap.set(r.codigo, r.id));
  }

  // -------- 2) INQUILINOS (crear faltantes, luego mapear)
  const allNames = [...new Set(items.map((x) => x.inquilinoNombre))];

  // traer existentes
  const inqMap = new Map();
  for (const part of chunk(allNames, 500)) {
    const { data, error } = await sb.from("inquilinos").select("id,nombre").in("nombre", part);
    if (error) throw error;
    data.forEach((r) => inqMap.set(r.nombre, r.id));
  }

  // insertar faltantes
  const missing = allNames.filter((n) => !inqMap.has(n)).map((n) => ({ nombre: n }));
  for (const part of chunk(missing, 500)) {
    const { error } = await sb.from("inquilinos").insert(part);
    if (error) throw error;
  }

  // refrescar mapa
  for (const part of chunk(allNames, 500)) {
    const { data, error } = await sb.from("inquilinos").select("id,nombre").in("nombre", part);
    if (error) throw error;
    data.forEach((r) => inqMap.set(r.nombre, r.id));
  }

  // -------- 3) CONTRATOS + CUOTAS + AJUSTES + PAGOS (por unidad)
  let ok = 0;
  for (const x of items) {
    const localId = localesMap.get(x.codigo);
    const inqId = inqMap.get(x.inquilinoNombre);

    if (!localId || !inqId) continue;

    // primer mes con proyectado > 0
    const firstIdx = x.proyectado.findIndex((v) => v > 0);
    if (firstIdx === -1) continue;

    const startMonth = firstIdx + 1;
    const fechaInicio = dateStr(YEAR, startMonth, 1);
    const alquilerInicial = x.proyectado[firstIdx];

    // contrato activo para ese local
    let { data: contrato, error: cErr } = await sb
      .from("contratos")
      .select("id,dia_vencimiento,inquilino_id")
      .eq("local_id", localId)
      .eq("estado", "ACTIVO")
      .limit(1)
      .maybeSingle();
    if (cErr) throw cErr;

    if (!contrato) {
      const { data: ins, error: insErr } = await sb
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
        .select("id,dia_vencimiento")
        .single();
      if (insErr) throw insErr;
      contrato = ins;
    } else {
      // si cambió el inquilino, lo actualizamos
      if (contrato.inquilino_id !== inqId) {
        await sb.from("contratos").update({ inquilino_id: inqId }).eq("id", contrato.id);
      }
    }

    const contratoId = contrato.id;
    const diaVenc = contrato.dia_vencimiento || DEFAULT_DIA_VENC;

    // Ajustes: cada vez que cambie el proyectado vs mes anterior
    let prev = null;
    const ajustes = [];
    for (let m = 1; m <= 12; m++) {
      const val = x.proyectado[m - 1];
      if (val <= 0) continue;

      if (prev === null) prev = val;
      if (val !== prev) {
        ajustes.push({
          contrato_id: contratoId,
          vigente_desde: dateStr(YEAR, m, 1),
          tipo: "MONTO",
          valor: Number(val).toFixed(2),
          nota: "Import Balance 2026",
        });
        prev = val;
      }
    }

    if (ajustes.length) {
      for (const part of chunk(ajustes, 500)) {
        const { error } = await sb.from("contrato_ajustes")
          .upsert(part, { onConflict: "contrato_id,vigente_desde" });
        if (error) throw error;
      }
    }

    // Cuotas: PROYECTADO
    const cuotasPayload = [];
    for (let m = 1; m <= 12; m++) {
      const val = x.proyectado[m - 1];
      if (val <= 0) continue;

      const vencDay = Math.min(diaVenc, lastDay(YEAR, m));
      cuotasPayload.push({
        contrato_id: contratoId,
        periodo: dateStr(YEAR, m, 1),
        vencimiento: dateStr(YEAR, m, vencDay),
        alquiler_mes: Number(val).toFixed(2),
        total: Number(val).toFixed(2),
      });
    }

    for (const part of chunk(cuotasPayload, 500)) {
      const { error } = await sb.from("cuotas")
        .upsert(part, { onConflict: "contrato_id,periodo" });
      if (error) throw error;
    }

    // Pagos: RECAUDADO (se aplica FIFO con RPC)
    // Nota: Balance no trae fecha real de pago. Usamos el vencimiento del mes.
    for (let m = 1; m <= 12; m++) {
      const pay = x.recaudado[m - 1];
      if (pay <= 0) continue;

      const vencDay = Math.min(diaVenc, lastDay(YEAR, m));
      const fechaPago = dateStr(YEAR, m, vencDay);

      const { error } = await sb.rpc("registrar_pago_y_aplicar", {
        p_contrato_id: contratoId,
        p_fecha_pago: fechaPago,
        p_monto: Number(pay).toFixed(2),
        p_medio: "IMPORT_BALANCE",
        p_referencia: `BAL-${YEAR}-${String(m).padStart(2,"0")}`,
      });
      if (error) throw error;
    }

    ok++;
    if (ok % 25 === 0) console.log(`Importados ${ok}/${items.length}...`);
  }

  console.log(`✅ Import finalizado. Unidades importadas: ${ok}/${items.length}`);
}

main().catch((e) => {
  console.error("❌ Error:", e.message || e);
  process.exit(1);
});