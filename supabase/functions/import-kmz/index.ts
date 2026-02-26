import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { XMLParser } from "npm:fast-xml-parser@4.3.4";
import JSZip from "npm:jszip@3.10.1";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Client-Info, Apikey",
};

interface ImportResult {
  lineas_created: number;
  tramos_inserted: number;
  estructuras_inserted: number;
  lineas_finalized: number;
  errores: string[];
  warnings: string[];
}

function parseCoordinates(coordString: string): Array<[number, number]> {
  const coords: Array<[number, number]> = [];
  const parts = coordString.trim().split(/\s+/);

  for (const part of parts) {
    const values = part.split(",");
    if (values.length >= 2) {
      const lon = parseFloat(values[0]);
      const lat = parseFloat(values[1]);
      if (!isNaN(lon) && !isNaN(lat)) {
        coords.push([lon, lat]);
      }
    }
  }

  return coords;
}

function ensureArray<T>(value: T | T[] | undefined): T[] {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function getExtendedDataValue(extendedData: unknown, key: string): string | null {
  if (!extendedData || !extendedData.Data) return null;

  const dataArray = ensureArray(extendedData.Data);
  for (const data of dataArray) {
    if (data["@_name"] === key && data.value) {
      return data.value.toString().trim();
    }
  }

  return null;
}

async function processFolderStructure(
  document: unknown,
  supabase: unknown,
  result: ImportResult
) {
  const folders = ensureArray(document.Folder);

  for (const folder of folders) {
    const folderName = folder.name;
    if (!folderName) continue;

    const lineaNumero = folderName.toString().trim();

    const { data: existingLinea } = await supabase
      .from("lineas")
      .select("id")
      .eq("numero", lineaNumero)
      .maybeSingle();

    let lineaId: string;

    if (existingLinea) {
      lineaId = existingLinea.id;
      await supabase.from("linea_tramos").delete().eq("linea_id", lineaId);
      await supabase.from("estructuras").delete().eq("linea_id", lineaId);
    } else {
      const { data: newLinea, error } = await supabase
        .from("lineas")
        .insert({
          numero: lineaNumero,
          nombre: lineaNumero,
        })
        .select("id")
        .single();

      if (error || !newLinea) {
        result.errores.push(
          `Failed to create linea ${lineaNumero}: ${error?.message || "Unknown error"}`
        );
        continue;
      }

      lineaId = newLinea.id;
      result.lineas_created++;
    }

    const subFolders = ensureArray(folder.Folder);
    let lineaAereaFolder: unknown = null;
    let estructurasFolder: unknown = null;

    for (const subFolder of subFolders) {
      const subFolderName = subFolder.name?.toString().toLowerCase() || "";
      if (subFolderName.includes("lineaarea") || subFolderName.includes("linea")) {
        lineaAereaFolder = subFolder;
      } else if (subFolderName.includes("estructura")) {
        estructurasFolder = subFolder;
      }
    }

    if (lineaAereaFolder) {
      const placemarks = ensureArray(lineaAereaFolder.Placemark);
      let orden = 0;

      for (const placemark of placemarks) {
        const lineString = placemark.LineString;
        if (!lineString?.coordinates) continue;

        const coordsText = lineString.coordinates.toString().trim();
        const coords = parseCoordinates(coordsText);

        if (coords.length < 2) continue;

        const lineWKT = `LINESTRING(${coords.map((c) => `${c[0]} ${c[1]}`).join(", ")})`;

        const { error } = await supabase.from("linea_tramos").insert({
          linea_id: lineaId,
          orden: orden++,
          geom: lineWKT,
        });

        if (error) {
          result.errores.push(
            `Failed to insert tramo for linea ${lineaNumero}: ${error.message}`
          );
        } else {
          result.tramos_inserted++;
        }
      }
    }

    if (estructurasFolder) {
      const placemarks = ensureArray(estructurasFolder.Placemark);

      for (const placemark of placemarks) {
        const name = placemark.name?.toString().trim();
        const point = placemark.Point;

        if (!name || !point?.coordinates) continue;

        const coordsText = point.coordinates.toString().trim();
        const coords = parseCoordinates(coordsText);

        if (coords.length === 0) continue;

        const [lon, lat] = coords[0];
        const pointWKT = `POINT(${lon} ${lat})`;

        const { error } = await supabase.from("estructuras").insert({
          linea_id: lineaId,
          numero_estructura: name,
          km: 0,
          geom: pointWKT,
        });

        if (error) {
          result.errores.push(
            `Failed to insert estructura ${name} for linea ${lineaNumero}: ${error.message}`
          );
        } else {
          result.estructuras_inserted++;
        }
      }
    }

    if (result.tramos_inserted > 0) {
      const { error: finalizeError } = await supabase.rpc(
        "finalize_kmz_import_for_linea",
        {
          p_linea_id: lineaId,
        }
      );

      if (finalizeError) {
        result.errores.push(
          `Failed to finalize linea ${lineaNumero}: ${finalizeError.message}`
        );
      } else {
        result.lineas_finalized++;
      }
    } else {
      result.warnings.push(
        `No line segments found for linea ${lineaNumero}, skipping finalization`
      );
    }
  }
}

async function processPlacemarkStructure(
  document: unknown,
  supabase: unknown,
  result: ImportResult
) {
  const placemarks = ensureArray(document.Placemark);

  const lineasMap = new Map<string, { tramos: unknown[]; estructuras: unknown[] }>();

  for (const placemark of placemarks) {
    const lineaNumero = getExtendedDataValue(placemark.ExtendedData, "linea");

    if (!lineaNumero) continue;

    if (!lineasMap.has(lineaNumero)) {
      lineasMap.set(lineaNumero, { tramos: [], estructuras: [] });
    }

    const lineaData = lineasMap.get(lineaNumero)!;

    const isEstructura = getExtendedDataValue(placemark.ExtendedData, "estructura");

    if (placemark.LineString) {
      lineaData.tramos.push(placemark);
    // Nota: subestaciones se eliminaron del sistema. Puntos se importan como estructuras.
    } else if (placemark.Point && isEstructura) {
      lineaData.estructuras.push(placemark);
    }
  }

  for (const [lineaNumero, data] of lineasMap) {
    const { data: existingLinea } = await supabase
      .from("lineas")
      .select("id")
      .eq("numero", lineaNumero)
      .maybeSingle();

    let lineaId: string;

    if (existingLinea) {
      lineaId = existingLinea.id;
      await supabase.from("linea_tramos").delete().eq("linea_id", lineaId);
      await supabase.from("estructuras").delete().eq("linea_id", lineaId);
    } else {
      const { data: newLinea, error } = await supabase
        .from("lineas")
        .insert({
          numero: lineaNumero,
          nombre: lineaNumero,
        })
        .select("id")
        .single();

      if (error || !newLinea) {
        result.errores.push(
          `Failed to create linea ${lineaNumero}: ${error?.message || "Unknown error"}`
        );
        continue;
      }

      lineaId = newLinea.id;
      result.lineas_created++;
    }

    let orden = 0;
    for (const tramoPlacemark of data.tramos) {
      const coordsText = tramoPlacemark.LineString.coordinates.toString().trim();
      const coords = parseCoordinates(coordsText);

      if (coords.length < 2) continue;

      const lineWKT = `LINESTRING(${coords.map((c) => {
        const p = c as [number, number];
        return `${p[0]} ${p[1]}`;
      }).join(", ")})`;

      const { error } = await supabase.from("linea_tramos").insert({
        linea_id: lineaId,
        orden: orden++,
        geom: lineWKT,
      });

      if (error) {
        result.errores.push(
          `Failed to insert tramo for linea ${lineaNumero}: ${error.message}`
        );
      } else {
        result.tramos_inserted++;
      }
    }

    for (const estructuraPlacemark of data.estructuras) {
      const name = estructuraPlacemark.name?.toString().trim();
      const coordsText = estructuraPlacemark.Point.coordinates.toString().trim();
      const coords = parseCoordinates(coordsText);

      if (coords.length === 0 || !name) continue;

      const [lon, lat] = coords[0];
      const pointWKT = `POINT(${lon} ${lat})`;

      const { error } = await supabase.from("estructuras").insert({
        linea_id: lineaId,
        numero_estructura: name,
        km: 0,
        geom: pointWKT,
      });

      if (error) {
        result.errores.push(
          `Failed to insert estructura ${name} for linea ${lineaNumero}: ${error.message}`
        );
      } else {
        result.estructuras_inserted++;
      }
    }

    if (data.tramos.length > 0) {
      const { error: finalizeError } = await supabase.rpc(
        "finalize_kmz_import_for_linea",
        {
          p_linea_id: lineaId,
        }
      );

      if (finalizeError) {
        result.errores.push(
          `Failed to finalize linea ${lineaNumero}: ${finalizeError.message}`
        );
      } else {
        result.lineas_finalized++;
      }
    } else {
      result.warnings.push(
        `No line segments found for linea ${lineaNumero}, skipping finalization`
      );
    }
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 200,
      headers: corsHeaders,
    });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseKey);

    const formData = await req.formData();
    const file = formData.get("file") as File;

    if (!file) {
      return new Response(
        JSON.stringify({ error: "No file provided" }),
        {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        }
      );
    }

    if (!file.name.endsWith(".kmz") && !file.name.endsWith(".kml")) {
      return new Response(
        JSON.stringify({ error: "File must be .kmz or .kml" }),
        {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        }
      );
    }

    let kmlContent: string;

    if (file.name.endsWith(".kmz")) {
      const arrayBuffer = await file.arrayBuffer();
      const zip = await JSZip.loadAsync(arrayBuffer);

      const kmlFiles: Array<{ name: string; content: string }> = [];

      for (const [filename, zipEntry] of Object.entries(zip.files)) {
        if (!zipEntry.dir && filename.toLowerCase().endsWith(".kml")) {
          const content = await zipEntry.async("string");
          kmlFiles.push({ name: filename, content });
        }
      }

      if (kmlFiles.length === 0) {
        throw new Error("No KML file found in KMZ");
      }

      kmlFiles.sort((a, b) => b.content.length - a.content.length);
      kmlContent = kmlFiles[0].content;
    } else {
      kmlContent = await file.text();
    }

    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      parseTagValue: false,
      trimValues: true,
    });

    const kmlData = parser.parse(kmlContent);

    const document = kmlData?.kml?.Document;
    if (!document) {
      throw new Error("Invalid KML structure: no Document element found");
    }

    const result: ImportResult = {
      lineas_created: 0,
      tramos_inserted: 0,
      estructuras_inserted: 0,
      lineas_finalized: 0,
      errores: [],
      warnings: [],
    };

    if (document.Folder) {
      await processFolderStructure(document, supabase, result);
    } else if (document.Placemark) {
      await processPlacemarkStructure(document, supabase, result);
    } else {
      throw new Error("No Folders or Placemarks found in KML document");
    }

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error("Import KMZ error:", error);
    return new Response(
      JSON.stringify({
        error: error instanceof Error ? error.message : "Unknown error",
        stack: error instanceof Error ? error.stack : String(error),
      }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  }
});
