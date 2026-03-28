const noLocalCodes = [];

require("dotenv").config({ path: ".env.import" });
const XLSX = require("xlsx");
const { createClient } = require("@supabase/supabase-js");

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

const FILE = process.argv[2] || "COBRO DE ALQUILERS 2026.xlsx";

const norm = (v) => String(v ?? "").trim();

function normalizeCodigoFromSheetName(name) {
  let s = String(name || "").toUpperCase().trim();
  s = s.replace(/\s+/g, "");
  s = s.replace(/[-_]/g, "");

  if (s.includes("KIOSKO")) return "KIOSKO";
  if (s.includes("LAVADERO")) return "LAVADERO";
  if (s.includes("REMIS")) return "REMIS";

  const m = s.match(/^(PC|P|L|I)(\d+)$/);
  if (m) return m[1] + String(Number(m[2])).padStart(3, "0");

  return s;
}

function extractInquilino(rows) {
  const bad = (x) => {
    const u = String(x || "").toUpperCase();
    return (
      !u ||
      u.includes("PAGOS DEL ALQUILER") ||
      u.includes("ALQUILER") ||
      u.includes("RECIBO") ||
      u.includes("SALDO") ||
      u.includes("MES") ||
      u.includes("COSTO")
    );
  };

  // 1) Intento rápido: posiciones típicas
  const candidates = [
    rows?.[0]?.[2], rows?.[0]?.[1], rows?.[1]?.[2], rows?.[1]?.[1],
    rows?.[2]?.[2], rows?.[2]?.[1]
  ].map(norm).filter(x => x && x.length >= 3 && !bad(x));

  if (candidates.length) return candidates[0];

  // 2) Buscar etiquetas en un área más grande
  const maxR = Math.min(rows.length, 40);
  const maxC = 20;

  const keys = ["LOCATARIO", "INQUILINO", "NOMBRE"];

  for (let i = 0; i < maxR; i++) {
    const row = rows[i] || [];
    for (let j = 0; j < Math.min(row.length, maxC); j++) {
      const cell = norm(row[j]).toUpperCase();

      if (keys.some(k => cell.includes(k))) {
        // derecha
        const right = norm(row[j + 1]);
        if (right && right.length >= 3 && !bad(right)) return right;

        // dos a la derecha
        const right2 = norm(row[j + 2]);
        if (right2 && right2.length >= 3 && !bad(right2)) return right2;

        // debajo
        const down = norm((rows[i + 1] || [])[j]);
        if (down && down.length >= 3 && !bad(down)) return down;

        // debajo a la derecha
        const downRight = norm((rows[i + 1] || [])[j + 1]);
        if (downRight && downRight.length >= 3 && !bad(downRight)) return downRight;
      }
    }
  }

  // 3) Último recurso: buscar un string "razonable" en primeras filas
  for (let i = 0; i < maxR; i++) {
    const row = rows[i] || [];
    for (let j = 0; j < Math.min(row.length, maxC); j++) {
      const v = norm(row[j]);
      if (v.length >= 5 && v.length <= 60 && !bad(v)) {
        // filtro simple: que no sea número puro
        if (!/^\d+([.,]\d+)?$/.test(v)) return v;
      }
    }
  }

  return null;
}

async function getOrCreateInquilino(nombre) {
  const q = await sb.from("inquilinos").select("id").eq("nombre", nombre).limit(1).maybeSingle();
  if (q.error) throw q.error;
  if (q.data?.id) return q.data.id;

  const ins = await sb.from("inquilinos").insert({ nombre }).select("id").single();
  if (ins.error) throw ins.error;
  return ins.data.id;
}

(async () => {
  const wb = XLSX.readFile(FILE, { cellDates: true });

  let updated = 0, skippedNoName = 0, skippedNoLocal = 0;

  for (const sheetName of wb.SheetNames) {
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    const hasPagos = rows.some(r => norm(r?.[0]).toUpperCase().includes("PAGOS DEL ALQUILER"));
    if (!hasPagos) continue;

    const codigo = normalizeCodigoFromSheetName(sheetName);
    const nombre = extractInquilino(rows);
    if (!nombre) { skippedNoName++; continue; }

    const local = await sb.from("locales").select("id").eq("codigo", codigo).limit(1).maybeSingle();
    if (local.error) throw local.error;
    if (!local.data?.id) { skippedNoLocal++; noLocalCodes.push(codigo); continue; }

    const inqId = await getOrCreateInquilino(nombre);

    const upd = await sb
      .from("contratos")
      .update({ inquilino_id: inqId })
      .eq("local_id", local.data.id)
      .eq("estado", "ACTIVO");

    if (upd.error) throw upd.error;

    updated++;
    if (updated % 25 === 0) console.log(`Actualizados ${updated}...`);
  }

  console.log(`✅ FIN. Contratos actualizados: ${updated}`);
  console.log(`Saltadas sin nombre: ${skippedNoName}`);
  console.log(`Saltadas sin local en DB: ${skippedNoLocal}`);

console.log("Codigos sin local en DB:", [...new Set(noLocalCodes)]);

})();