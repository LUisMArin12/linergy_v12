// src/lib/lineaCatalog.ts
// Catálogo de características técnicas de líneas (fuente: CENACE / Catálogo proporcionado por el usuario)

export type LineaCatalogEntry = {
  claveEnlace: string;
  /** Número CENACE extraído de la clave (ej. 73200) */
  numero: string;
  descripcion: string;
  area: string;
  tension: string;
  kms: number | null;
  nc: number | null;
  conductor: string;
  tipoEstructura: string;
  numEstructuras: number | null;
  anio: number | null;
  comp: string;
  cveSap: string;
  brecha: number | null;
  confCond: string;
  pob: string;
  ent: string;
};

export const LINEAS_CATALOG: LineaCatalogEntry[] = [
  {
    "claveEnlace": "MIL-73200-DGU",
    "numero": "73200",
    "descripcion": "MILENIO-DURANGO UNO",
    "area": "6",
    "tension": "115 kV",
    "kms": 11.6,
    "nc": 0,
    "conductor": "477-ACSR",
    "tipoEstructura": "IS",
    "numEstructuras": 81,
    "anio": 2018,
    "comp": "N",
    "cveSap": "0000",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "CLI-73290-CMI",
    "numero": "73290",
    "descripcion": "CENTRO LOG. IND. DE DURANGO -",
    "area": "6",
    "tension": "115 kV",
    "kms": 10.68,
    "nc": 0,
    "conductor": "477-ACSR",
    "tipoEstructura": "1210SME",
    "numEstructuras": 65,
    "anio": 2020,
    "comp": "S",
    "cveSap": "0000",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "CLI-73330-GVR",
    "numero": "73330",
    "descripcion": "CENTRO LOGÍSTICO E INDUSTRIAL DE",
    "area": "6",
    "tension": "115 kV",
    "kms": 48.45,
    "nc": 0,
    "conductor": "336-ACSR",
    "tipoEstructura": "1210MEL",
    "numEstructuras": 219,
    "anio": 2015,
    "comp": "S",
    "cveSap": "P358",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "D",
    "ent": "D"
  },
  {
    "claveEnlace": "AMN-73350-CMP",
    "numero": "73350",
    "descripcion": "AMADO NERVO - MANIOBRAS PARRILLA",
    "area": "6",
    "tension": "115 kV",
    "kms": 15.27,
    "nc": 0,
    "conductor": "4/0-ACSR-AW",
    "tipoEstructura": "1211DML",
    "numEstructuras": 64,
    "anio": 2015,
    "comp": "S",
    "cveSap": "P353",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "MAC-73370-AMN",
    "numero": "73370",
    "descripcion": "MANIOBRAS CENTAURO - AMADO NERVO",
    "area": "6",
    "tension": "115 kV",
    "kms": 19.75,
    "nc": 40,
    "conductor": "4/0-ACSR",
    "tipoEstructura": "1210MEL",
    "numEstructuras": 128,
    "anio": 1965,
    "comp": "N",
    "cveSap": "P326",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "DGD-73390-DGU",
    "numero": "73390",
    "descripcion": "DURANGO DOS - DURANGO UNO",
    "area": "6",
    "tension": "115 kV",
    "kms": 6.92,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1213DMP",
    "numEstructuras": 39,
    "anio": 1976,
    "comp": "N",
    "cveSap": "P060",
    "brecha": 7.06,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73400-MIL",
    "numero": "73400",
    "descripcion": "JERONIMO ORTIZ - MILENIO",
    "area": "6",
    "tension": "115 kV",
    "kms": 10.76,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "PMOIII",
    "numEstructuras": 58,
    "anio": 2018,
    "comp": "N",
    "cveSap": "P068",
    "brecha": 20.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "DGD-73420-CME",
    "numero": "73420",
    "descripcion": "DURANGO DOS - CERRO DEL MERCADO",
    "area": "6",
    "tension": "115 kV",
    "kms": 6.15,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "MS",
    "numEstructuras": 24,
    "anio": 1976,
    "comp": "N",
    "cveSap": "P064",
    "brecha": 30.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73430-CLI",
    "numero": "73430",
    "descripcion": "JERONIMO ORTIZ - CENTRO LOGÍSTICO E",
    "area": "6",
    "tension": "115 kV",
    "kms": 20.49,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "MR",
    "numEstructuras": 84,
    "anio": 1982,
    "comp": "N",
    "cveSap": "P063",
    "brecha": 20.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "DGD-73440-MAC",
    "numero": "73440",
    "descripcion": "DURANGO DOS - MANIOBRAS CENTAURO",
    "area": "6",
    "tension": "115 kV",
    "kms": 18.42,
    "nc": 40,
    "conductor": "336-ACSR",
    "tipoEstructura": "MR",
    "numEstructuras": 80,
    "anio": 1965,
    "comp": "N",
    "cveSap": "P067",
    "brecha": 20.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CND-73460-NID",
    "numero": "73460",
    "descripcion": "CANATLAN DOS - NUEVO IDEAL",
    "area": "6",
    "tension": "115 kV",
    "kms": 55.54,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "MR",
    "numEstructuras": 212,
    "anio": 1977,
    "comp": "N",
    "cveSap": "P061",
    "brecha": 5.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "SGP-73470-ATR",
    "numero": "73470",
    "descripcion": "SANTIAGO PAPASQUIARO - ALTARES",
    "area": "6",
    "tension": "115 kV",
    "kms": 56.1,
    "nc": 40,
    "conductor": "336-ACSR",
    "tipoEstructura": "MSR",
    "numEstructuras": 283,
    "anio": 1983,
    "comp": "N",
    "cveSap": "P035",
    "brecha": 5.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "NID-73480-SGP",
    "numero": "73480",
    "descripcion": "NUEVO IDEAL - SANTIAGO PAPASQUIARO",
    "area": "6",
    "tension": "115 kV",
    "kms": 64.81,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "MR",
    "numEstructuras": 279,
    "anio": 1984,
    "comp": "N",
    "cveSap": "P135",
    "brecha": 30.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "MCM-73490-CDM",
    "numero": "73490",
    "descripcion": "MERCADO MANIOBRAS - MINA CERRO",
    "area": "6",
    "tension": "115 kV",
    "kms": 39.34,
    "nc": 40,
    "conductor": "336-ACSR",
    "tipoEstructura": "PMR",
    "numEstructuras": 166,
    "anio": 1983,
    "comp": "N",
    "cveSap": "P103",
    "brecha": 5.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "ATR-73500-CID",
    "numero": "73500",
    "descripcion": "ALTARES - CIENEGA DOS",
    "area": "6",
    "tension": "115 kV",
    "kms": 0.3,
    "nc": 40,
    "conductor": "336-ACSR",
    "tipoEstructura": "IS",
    "numEstructuras": 3,
    "anio": 1976,
    "comp": "N",
    "cveSap": "P099",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CID-73530-CIE",
    "numero": "73530",
    "descripcion": "CIENEGA DOS - CIENEGA",
    "area": "6",
    "tension": "115 kV",
    "kms": 53.19,
    "nc": 40,
    "conductor": "266-ACSR",
    "tipoEstructura": "MRG",
    "numEstructuras": 183,
    "anio": 1994,
    "comp": "N",
    "cveSap": "P193",
    "brecha": 52.85,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CMI-73540-VGR",
    "numero": "73540",
    "descripcion": "INTERNACIONAL WIRE - VICENTE GUERRERO",
    "area": "6",
    "tension": "115 kV",
    "kms": 53.1,
    "nc": 40,
    "conductor": "266-ACSR",
    "tipoEstructura": "PMR",
    "numEstructuras": 198,
    "anio": 2002,
    "comp": "N",
    "cveSap": "P163",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CAP-73550-CND",
    "numero": "73550",
    "descripcion": "CANATLAN POTENCIA-CANATLAN DOS",
    "area": "6",
    "tension": "115 kV",
    "kms": 3.51,
    "nc": 40,
    "conductor": "266-ACSR",
    "tipoEstructura": "PMR",
    "numEstructuras": 14,
    "anio": 1994,
    "comp": "N",
    "cveSap": "P194",
    "brecha": 4.2,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "VGR-73560-CHA",
    "numero": "73560",
    "descripcion": "VICENTE GUERRERO - CHARCOS",
    "area": "6",
    "tension": "115 kV",
    "kms": 87.67,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1219DMP",
    "numEstructuras": 497,
    "anio": 1997,
    "comp": "N",
    "cveSap": "P296",
    "brecha": 5.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "VGR-73570-CMP",
    "numero": "73570",
    "descripcion": "VICENTE GUERRERO - MANIOBRAS",
    "area": "M",
    "tension": "115 kV",
    "kms": 8.27,
    "nc": 0,
    "conductor": "1113-ACSR",
    "tipoEstructura": "TASZP",
    "numEstructuras": 24,
    "anio": 2017,
    "comp": "S",
    "cveSap": "0000",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CAP-73650-CND",
    "numero": "73650",
    "descripcion": "CANATLAN POTENCIA-CANATLAN DOS",
    "area": "6",
    "tension": "115 kV",
    "kms": 88.28,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1213DMP",
    "numEstructuras": 454,
    "anio": 2006,
    "comp": "N",
    "cveSap": "P216",
    "brecha": 88.23,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "MVC-73870-QDO",
    "numero": "73870",
    "descripcion": "MANIOBRAS VERSALLES DE LAS CUATAS -",
    "area": "M",
    "tension": "115 kV",
    "kms": 10.62,
    "nc": 40,
    "conductor": "4/0-ACSR",
    "tipoEstructura": "1219DML",
    "numEstructuras": 42,
    "anio": 1965,
    "comp": "N",
    "cveSap": "P294",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "CME-73890-MCM",
    "numero": "73890",
    "descripcion": "CERRO DEL MERCADO - MERCADO",
    "area": "6",
    "tension": "115 kV",
    "kms": 8.07,
    "nc": 0,
    "conductor": "477-ACSR",
    "tipoEstructura": "TARZP",
    "numEstructuras": 23,
    "anio": 2017,
    "comp": "S",
    "cveSap": "0000",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "MCM-73900-GDN",
    "numero": "73900",
    "descripcion": "MERCADO MANIOBRAS - GUADIANA",
    "area": "6",
    "tension": "115 kV",
    "kms": 8.19,
    "nc": 0,
    "conductor": "477",
    "tipoEstructura": "1213DMP",
    "numEstructuras": 44,
    "anio": 2020,
    "comp": "S",
    "cveSap": "0000",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "GDN-73920-ESL",
    "numero": "73920",
    "descripcion": "GUADIANA - EL SALTO",
    "area": "6",
    "tension": "115 kV",
    "kms": 1.68,
    "nc": 40,
    "conductor": "1113-ACSR",
    "tipoEstructura": "1219DMP",
    "numEstructuras": 19,
    "anio": 1976,
    "comp": "N",
    "cveSap": "P292",
    "brecha": 1.87,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "DGD-73940-CMI",
    "numero": "73940",
    "descripcion": "DURANGO DOS - INTERNACIONAL WIRE",
    "area": "6",
    "tension": "115 kV",
    "kms": 8.05,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "IS",
    "numEstructuras": 60,
    "anio": 1982,
    "comp": "N",
    "cveSap": "P037",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "DGD-73950-CYP",
    "numero": "73950",
    "descripcion": "DURANGO DOS - CELULOSA Y PAPEL",
    "area": "6",
    "tension": "115 kV",
    "kms": 103.0,
    "nc": 40,
    "conductor": "336-ACSR",
    "tipoEstructura": "PMOIII",
    "numEstructuras": 372,
    "anio": 1983,
    "comp": "N",
    "cveSap": "P086",
    "brecha": 80.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73960-DGU",
    "numero": "73960",
    "descripcion": "JERONIMO ORTIZ - DURANGO UNO",
    "area": "6",
    "tension": "115 kV",
    "kms": 0.84,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "PMR",
    "numEstructuras": 10,
    "anio": 1986,
    "comp": "N",
    "cveSap": "P187",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "GDN-73970-QDO",
    "numero": "73970",
    "descripcion": "GUADIANA - QUINCE DE OCTUBRE",
    "area": "6",
    "tension": "115 kV",
    "kms": 6.45,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1213DMP",
    "numEstructuras": 60,
    "anio": 2003,
    "comp": "N",
    "cveSap": "P062",
    "brecha": 0.0,
    "confCond": "HORIZONTAL",
    "pob": "R",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73980-MVC",
    "numero": "73980",
    "descripcion": "JERONIMO ORTIZ - MANIOBRAS VERSALLES",
    "area": "6",
    "tension": "115 kV",
    "kms": 13.17,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "PMR",
    "numEstructuras": 98,
    "anio": 1996,
    "comp": "N",
    "cveSap": "P295",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73990-LAF",
    "numero": "73990",
    "descripcion": "JERONIMO ORTIZ - LA FLOR",
    "area": "6",
    "tension": "115 kV",
    "kms": 10.82,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1211DNE",
    "numEstructuras": 111,
    "anio": 1998,
    "comp": "N",
    "cveSap": "P195",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  },
  {
    "claveEnlace": "JOM-73990-LAF",
    "numero": "73990",
    "descripcion": "JERONIMO ORTIZ - LA FLOR",
    "area": "6",
    "tension": "115 kV",
    "kms": 10.98,
    "nc": 40,
    "conductor": "477-ACSR",
    "tipoEstructura": "1416RNE",
    "numEstructuras": 25,
    "anio": 2020,
    "comp": "N",
    "cveSap": "P293",
    "brecha": 0.0,
    "confCond": "VERTICAL",
    "pob": "U",
    "ent": "D"
  }
] as const;

function norm(s: string) {
  return (s ?? '')
    .toString()
    .trim()
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .replace(/[^A-Z0-9-]/g, '');
}

/** Extrae el primer bloque numérico de 5 dígitos (si existe) */
export function extractLineaNumero(value: string | null | undefined): string | null {
  if (!value) return null;
  const m = value.toString().match(/(\d{5})/);
  return m ? m[1] : null;
}

export function findLineaCatalogEntry(lineaNumeroOrClave: string | null | undefined): LineaCatalogEntry | null {
  if (!lineaNumeroOrClave) return null;

  const raw = lineaNumeroOrClave.toString();
  const n = extractLineaNumero(raw);
  const nNorm = n ? norm(n) : null;

  // 1) match directo por número (más robusto para linea.numero en BD)
  if (nNorm) {
    const byNum = LINEAS_CATALOG.find((e) => norm(e.numero) === nNorm);
    if (byNum) return byNum;
  }

  // 2) match por clave completa
  const k = norm(raw);
  const byClave = LINEAS_CATALOG.find((e) => norm(e.claveEnlace) === k);
  if (byClave) return byClave;

  // 3) fallback: contiene número dentro de clave
  if (nNorm) {
    const byContains = LINEAS_CATALOG.find((e) => norm(e.claveEnlace).includes(nNorm));
    if (byContains) return byContains;
  }

  return null;
}

export function formatLineaCatalogEntryLines(entry: LineaCatalogEntry): string[] {
  return [
    `CLAVE ENLACE: ${entry.claveEnlace}`,
    `DESCRIPCIÓN: ${entry.descripcion}`,
    `ÁREA: ${entry.area}`,
    `TENSIÓN: ${entry.tension}`,
    `KMS: ${entry.kms ?? 'N/A'}`,
    `NC: ${entry.nc ?? 'N/A'}`,
    `CONDUCTOR: ${entry.conductor}`,
    `TIP. ESTRUC: ${entry.tipoEstructura}`,
    `# EST: ${entry.numEstructuras ?? 'N/A'}`,
    `AÑO: ${entry.anio ?? 'N/A'}`,
    `COMP: ${entry.comp}`,
    `CVE SAP: ${entry.cveSap}`,
    `BRECHA: ${entry.brecha ?? 'N/A'}`,
    `CONF COND: ${entry.confCond}`,
    `POB: ${entry.pob}`,
    `ENT: ${entry.ent}`,
  ];
}

export function formatLineaCatalogEntryInline(entry: LineaCatalogEntry): string {
  return `${entry.claveEnlace} · ${entry.tension} · ${entry.kms ?? 'N/A'} km · Conductor ${entry.conductor} · ${entry.confCond} · SAP ${entry.cveSap}`;
}
