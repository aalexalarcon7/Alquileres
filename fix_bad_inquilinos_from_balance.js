require("dotenv").config({ path: ".env.import" });
const XLSX = require("xlsx");
const { createClient } = require("@supabase/supabase-js");

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

const FILE = process.argv[2] || "Balance 2026.xlsx";
const YEAR = Number(process.argv[3] || 2026);
const SHEET = "DEUDAS, RECAUDADO Y PROYECTADO";

const norm = (v) => String(v ?? "").trim();

const conceptToCode = (concept, nro) => {
  const n = String(Number(nro)).padStart(3, "0");
  const c = String(concept || "").trim().toLowerCase();
  if (c === "puestos") return `P${n}`;
  if (c === "local") return `L${n}`;
  if (c === "isla") return `I${n}`;
  if (c === "patio de comida") return `PC${n}`;
  if (c === "kiosko") return `KIOSKO`;
  if (c === "lavadero") return `LAVADERO`;
  if (c === "remis") return `REMIS`;
  return `${concept}-${n}`;
};

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
  const ws = wb.Sheets[SHEET];
  if (!ws) throw new Error(`No existe la hoja "${SHEET}"`);

  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  // índices 0-based del Balance (según tu import)
  const SALDO_CONCEPT = 0, SALDO_NRO = 1, SALDO_LOC = 2;
  const RECAUDADO_LOC = 19;
  const PROYECTADO_LOC = 36;

  let fixed = 0, skippedNoName = 0, skippedNoLocal = 0;

  for (let i = 3; i < rows.length; i++) {
    const concept = rows[i][SALDO_CONCEPT];
    const nro = rows[i][SALDO_NRO];
    if (typeof concept !== "string") continue;

    const conceptClean = concept.trim();
    if (!conceptClean || conceptClean.toUpperCase() === "TOTAL MES") continue;
    if (nro === null || nro === undefined || nro === "") continue;

    const codigo = conceptToCode(conceptClean, nro);

    const locName =
      norm(rows[i][SALDO_LOC]) ||
      norm(rows[i][RECAUDADO_LOC]) ||
      norm(rows[i][PROYECTADO_LOC]);

    if (!locName) { skippedNoName++; continue; }

    // buscar local
    const local = await sb.from("locales").select("id").eq("codigo", codigo).limit(1).maybeSingle();
    if (local.error) throw local.error;
    if (!local.data?.id) { skippedNoLocal++; continue; }

    const inqId = await getOrCreateInquilino(locName);

    // actualizar SOLO si hoy está mal (PAGO...) o SIN ASIGNAR
    const upd = await sb
      .from("contratos")
      .update({ inquilino_id: inqId })
      .eq("local_id", local.data.id)
      .eq("estado", "ACTIVO")
      .in("inquilino_id", (
        await sb.from("inquilinos")
          .select("id")
          .or("nombre.ilike.PAGO%,nombre.ilike.SIN ASIGNAR -%")
      ).data?.map(x => x.id) || [] );

    // Nota: el update de arriba puede quedar vacío si el "in" no trae ids; hacemos un fallback simple:
    if (upd.error) {
      // fallback: actualizar si el nombre actual es PAGO% o SIN ASIGNAR%
      const current = await sb
        .from("contratos")
        .select("id, inquilinos(nombre)")
        .eq("local_id", local.data.id)
        .eq("estado", "ACTIVO")
        .limit(1)
        .maybeSingle();

      if (!current.error && current.data?.inquilinos?.nombre) {
        const nActual = current.data.inquilinos.nombre;
        if (/^PAGO/i.test(nActual) || /^SIN ASIGNAR/i.test(nActual)) {
          const u2 = await sb.from("contratos").update({ inquilino_id: inqId }).eq("id", current.data.id);
          if (u2.error) throw u2.error;
          fixed++;
        }
      }
    } else {
      fixed++;
    }
  }

  console.log("✅ FIN");
  console.log("Corregidos:", fixed);
  console.log("Sin nombre en Balance:", skippedNoName);
  console.log("Sin local en DB:", skippedNoLocal);
})().catch((e) => {
  console.error("❌", e.message || e);
  process.exit(1);
});