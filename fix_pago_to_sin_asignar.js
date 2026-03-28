require("dotenv").config({ path: ".env.import" });
const { createClient } = require("@supabase/supabase-js");

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

async function getOrCreateInquilino(nombre) {
  const q = await sb.from("inquilinos").select("id").eq("nombre", nombre).limit(1).maybeSingle();
  if (q.error) throw q.error;
  if (q.data?.id) return q.data.id;

  const ins = await sb.from("inquilinos").insert({ nombre }).select("id").single();
  if (ins.error) throw ins.error;
  return ins.data.id;
}

(async () => {
  // Traer contratos cuyo inquilino es "PAGO..."
  const { data: rows, error } = await sb
    .from("contratos")
    .select("id, local_id, locales(codigo), inquilinos(nombre)")
    .eq("estado", "ACTIVO");

  if (error) throw error;

  const target = (rows || []).filter(r => {
    const n = r.inquilinos?.nombre || "";
    return /^PAGO/i.test(n);
  });

  console.log("Contratos con inquilino PAGO%:", target.length);

  let fixed = 0;
  for (const r of target) {
    const codigo = r.locales?.codigo;
    if (!codigo) continue;

    const nuevoNombre = `SIN ASIGNAR - ${codigo}`;
    const inqId = await getOrCreateInquilino(nuevoNombre);

    const upd = await sb.from("contratos").update({ inquilino_id: inqId }).eq("id", r.id);
    if (upd.error) throw upd.error;

    fixed++;
    if (fixed % 25 === 0) console.log("Corregidos", fixed);
  }

  console.log("✅ Listo. Corregidos:", fixed);

  // Verificación final
  const check = await sb
    .from("contratos")
    .select("id, inquilinos(nombre)")
    .eq("estado", "ACTIVO");

  const still = (check.data || []).filter(r => /^PAGO/i.test(r.inquilinos?.nombre || "")).length;
  console.log("Quedan PAGO%:", still);
})().catch(e => {
  console.error("❌", e.message || e);
  process.exit(1);
});