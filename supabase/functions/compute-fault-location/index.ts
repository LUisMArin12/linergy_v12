import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Client-Info, Apikey, apikey",
};

interface ComputeLocationRequest {
  lineaId: string;
  km: number;
}

interface ComputeLocationResponse {
  lat: number;
  lon: number;
  geom: string;
  method: "interpolation" | "single_structure" | "line_geometry";
}

function firstRow<T>(data: unknown): T | null {
  if (!data) return null;
  if (Array.isArray(data)) return (data[0] ?? null) as T | null;
  return data as T;
}

function toNum(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}

type EstructuraRow = { km: number | string | null; geom: string | null };

function extractLatLon(data: unknown): { lat: number; lon: number } | null {
  const row = firstRow<Record<string, unknown>>(data);
  if (!row) return null;

  // soporta {lat, lon} o {lat, lng}
  const lat = toNum(row.lat ?? row.latitude);
  const lon = toNum(row.lon ?? row.lng ?? row.longitude);

  if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
  return null;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 200, headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseKey);

    const body = (await req.json()) as ComputeLocationRequest;
    const lineaId = body?.lineaId;
    const km = Number(body?.km);

    if (!lineaId || !Number.isFinite(km)) {
      return new Response(JSON.stringify({ error: "lineaId and km are required" }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const { data: linea, error: lineaError } = await supabase
      .from("lineas")
      .select("id, km_inicio, km_fin, geom")
      .eq("id", lineaId)
      .maybeSingle();

    if (lineaError || !linea) {
      return new Response(JSON.stringify({ error: "Line not found" }), {
        status: 404,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Validación rango si existe
    const kmInicio = linea.km_inicio !== null ? Number(linea.km_inicio) : null;
    const kmFin = linea.km_fin !== null ? Number(linea.km_fin) : null;

    if (kmInicio !== null && kmFin !== null && Number.isFinite(kmInicio) && Number.isFinite(kmFin)) {
      if (km < kmInicio || km > kmFin) {
        return new Response(
          JSON.stringify({ error: `km ${km} is out of range [${kmInicio}, ${kmFin}]` }),
          { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      }
    }

    // 1) Intento por estructuras (más preciso)
    const { data: estructuras, error: estructurasError } = await supabase
      .from("estructuras")
      .select("km, geom")
      .eq("linea_id", lineaId)
      .order("km", { ascending: true });

    if (estructurasError) throw estructurasError;

    if (estructuras && estructuras.length > 0) {
      let e1: EstructuraRow | null = null;
      let e2: EstructuraRow | null = null;

      for (const est of estructuras) {
        const estKm = Number(est.km);
        if (Number.isFinite(estKm) && estKm <= km) e1 = est;
        if (Number.isFinite(estKm) && estKm >= km && !e2) e2 = est;
      }

      // Interpolación entre dos estructuras
      if (e1 && e2 && Number(e1.km) !== Number(e2.km)) {
        const { data: coordsData, error } = await supabase.rpc("interpolate_point", {
          p_geom1: e1.geom,
          p_geom2: e2.geom,
          p_km1: Number(e1.km),
          p_km2: Number(e2.km),
          p_km_target: km,
        });
        if (error) throw error;

        const coords = extractLatLon(coordsData);
        if (coords) {
          return new Response(
            JSON.stringify({
              lat: coords.lat,
              lon: coords.lon,
              geom: `POINT(${coords.lon} ${coords.lat})`,
              method: "interpolation",
            } as ComputeLocationResponse),
            { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
          );
        }
      }

      // Estructura única (cercana)
      const single = e1 && !e2 ? e1 : !e1 && e2 ? e2 : null;
      if (single) {
        const { data: coordsData, error } = await supabase.rpc("get_point_coords", {
          p_geom: single.geom,
        });
        if (error) throw error;

        const coords = extractLatLon(coordsData);
        if (coords) {
          return new Response(
            JSON.stringify({
              lat: coords.lat,
              lon: coords.lon,
              geom: `POINT(${coords.lon} ${coords.lat})`,
              method: "single_structure",
            } as ComputeLocationResponse),
            { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
          );
        }
      }
    }

    // 2) Fallback por geometría de línea completa
    if (linea.geom && kmInicio !== null && kmFin !== null && kmFin > kmInicio) {
      let fraction = (km - kmInicio) / (kmFin - kmInicio);
      // clamp [0,1]
      fraction = Math.max(0, Math.min(1, fraction));

      const { data: coordsData, error } = await supabase.rpc("interpolate_line_point", {
        p_line_geom: linea.geom,
        p_fraction: fraction,
      });
      if (error) throw error;

      const coords = extractLatLon(coordsData);
      if (coords) {
        return new Response(
          JSON.stringify({
            lat: coords.lat,
            lon: coords.lon,
            geom: `POINT(${coords.lon} ${coords.lat})`,
            method: "line_geometry",
          } as ComputeLocationResponse),
          { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      }
    }

    return new Response(
      JSON.stringify({
        error: "Cannot compute location: no valid coords from structures or line geometry",
      }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error: unknown) {
    const msg = error?.message ?? String(error);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
