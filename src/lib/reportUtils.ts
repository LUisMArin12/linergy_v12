import { jsPDF } from 'jspdf';
import { findLineaCatalogEntry, formatLineaCatalogEntryLines } from './lineaCatalog';
import { Linea, parseGeometry } from './supabase';

import { GeoJSONGeometry } from '../types/geo';

export type FaultForReport = {
  id: string;
  ocurrencia_ts: string;
  km: number;
  tipo: string;
  descripcion: string | null;
  estado: string;
  geom: string | GeoJSONGeometry | null;
};

export type LineForReport = Pick<Linea, 'numero' | 'nombre'> | null;


function isFiniteNumber(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

function isValidLatLon(lat: unknown, lon: unknown): lat is number {
  return (
    isFiniteNumber(lat) &&
    isFiniteNumber(lon) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

export function generateFaultPDF(falla: FaultForReport, linea: LineForReport) {
  type JsPDFWithLink = jsPDF & { textWithLink?: (text: string, x: number, y: number, options: { url: string }) => void };

  const doc = new jsPDF({ unit: 'pt', format: 'a4' }) as JsPDFWithLink;

// Page layout
  const pageW = doc.internal.pageSize.getWidth();
  const pageH = doc.internal.pageSize.getHeight();
  const margin = 44;
  const contentW = pageW - margin * 2;

  let y = 58;

  // Institutional palette (sobria)
  const C = {
    text: [17, 24, 39], // slate-900
    subtext: [55, 65, 81], // slate-700
    muted: [100, 116, 139], // slate-500
    line: [203, 213, 225], // slate-300
    card: [250, 250, 250], // almost white (paper)
    brand: [21, 122, 90], // verde institucional aprox
    badgeText: [255, 255, 255],
  } as const;

  const setRGB = (rgb: readonly [number, number, number]) => doc.setTextColor(rgb[0], rgb[1], rgb[2]);
  const setDraw = (rgb: readonly [number, number, number]) => doc.setDrawColor(rgb[0], rgb[1], rgb[2]);
  const setFill = (rgb: readonly [number, number, number]) => doc.setFillColor(rgb[0], rgb[1], rgb[2]);

  const ensureSpace = (needed: number) => {
    if (y + needed > pageH - 72) {
      doc.addPage();
      y = 58;
      drawHeader(true);
    }
  };

  const fmtDateLong = (d: Date) =>
    d.toLocaleDateString('es-MX', { year: 'numeric', month: 'long', day: 'numeric' });

  const fmtTime = (d: Date) => d.toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit' });

  const estadoText =
    ({
      ABIERTA: 'Abierta',
      EN_ATENCION: 'En atención',
      CERRADA: 'Cerrada',
    } as const)[falla.estado] ?? falla.estado;

  const lineaText = `${linea?.numero || 'N/A'}${linea?.nombre ? ` — ${linea.nombre}` : ''}`;
  const kmText = Number.isFinite(falla.km) ? `${falla.km.toFixed(1)} km` : 'N/A';

  const ocurrencia = new Date(falla.ocurrencia_ts);
  const generado = new Date();

  // Coordinates: NO default a 0,0. Si no hay Point válido => null.
  const geom = parseGeometry(falla.geom ?? null);

  const coords = geom?.type === 'Point' ? geom.coordinates : null;

  const latRaw = coords ? Number(coords[1]) : null; // GeoJSON: [lon, lat]
  const lonRaw = coords ? Number(coords[0]) : null;

  const hasValidCoords = isValidLatLon(latRaw, lonRaw);
  const lat: number | null = hasValidCoords ? latRaw : null;
  const lon: number | null = hasValidCoords ? lonRaw : null;

  const coordsText = hasValidCoords && lat !== null && lon !== null ? `${lat.toFixed(6)}, ${lon.toFixed(6)}` : 'No disponible';

  const mapsUrl =
    hasValidCoords && lat !== null && lon !== null ? `https://www.google.com/maps?q=${lat},${lon}` : null;

  const folio = falla.id.slice(0, 8).toUpperCase();

  const drawHeader = (isContinuation = false) => {
    // Línea superior institucional
    setDraw(C.brand);
    doc.setLineWidth(3);
    doc.line(margin, 30, pageW - margin, 30);

    // Título
    setRGB(C.text);
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(15);
    doc.text('REPORTE DE FALLA', margin, 54);

    // Subtítulo institucional
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(10);
    setRGB(C.subtext);
    doc.text('Comisión Federal de Electricidad · Linergy', margin, 70);

    // Folio y emisión a la derecha
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(9);
    setRGB(C.muted);
    doc.text(`Folio: ${folio}`, pageW - margin, 54, { align: 'right' });
    doc.text(`Emisión: ${fmtDateLong(generado)} · ${fmtTime(generado)}`, pageW - margin, 70, {
      align: 'right',
    });

    // Separador
    setDraw(C.line);
    doc.setLineWidth(1);
    doc.line(margin, 84, pageW - margin, 84);

    if (isContinuation) {
      doc.setFontSize(9);
      setRGB(C.muted);
      doc.text('Continuación', pageW - margin, 98, { align: 'right' });
    }

    // Ajuste de y inicial para contenido
    y = 104;
  };

  const card = (h: number) => {
    ensureSpace(h + 12);

    // Card estilo “papel”
    setFill(C.card);
    setDraw(C.line);
    doc.setLineWidth(1);
    doc.roundedRect(margin, y, contentW, h, 6, 6, 'FD');

    // Barra lateral sutil institucional
    setDraw(C.brand);
    doc.setLineWidth(2);
    doc.line(margin + 8, y + 12, margin + 8, y + h - 12);

    const innerX = margin + 20;
    const innerY = y + 18;

    y += h + 14;
    return { x: innerX, y: innerY, w: contentW - 32 };
  };

  const sectionTitle = (x: number, yy: number, title: string) => {
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10.5);
    setRGB(C.text);
    doc.text(title.toUpperCase(), x, yy);

    setDraw(C.line);
    doc.setLineWidth(1);
    doc.line(x, yy + 8, x + 220, yy + 8);
  };

  const kvRow = (x: number, yy: number, label: string, value: string, valueMaxW: number) => {
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8.5);
    setRGB(C.muted);
    doc.text(label.toUpperCase(), x, yy);

    doc.setFont('helvetica', 'normal');
    doc.setFontSize(10.5);
    setRGB(C.text);

    const lines = doc.splitTextToSize(value || 'N/A', valueMaxW);
    doc.text(lines, x, yy + 16);

    return yy + 16 + lines.length * 13 + 8;
  };

  const badge = (text: string, x: number, yy: number) => {
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(9);

    const padX = 10;
    const tw = doc.getTextWidth(text);
    const bw = tw + padX * 2;
    const bh = 18;

    // Badge institucional (verde)
    setFill(C.brand);
    doc.roundedRect(x, yy - 12, bw, bh, 9, 9, 'F');

    setRGB(C.badgeText);
    doc.text(text, x + bw / 2, yy + 1, { align: 'center' });

    setRGB(C.text);
  };

  // ---- Document start ----
  drawHeader(false);

  // Resumen
  {
    const c = card(120);
    sectionTitle(c.x, c.y, 'Resumen');

    let yy = c.y + 24;
    const col1X = c.x;
    const col2X = c.x + c.w * 0.55;

    yy = kvRow(col1X, yy, 'ID', falla.id, c.w * 0.5);
    kvRow(col1X, yy, 'Línea', lineaText, c.w * 0.5);

    let yy2 = c.y + 24;
    yy2 = kvRow(col2X, yy2, 'Kilómetro', kmText, c.w * 0.4);
    kvRow(col2X, yy2, 'Tipo', falla.tipo || 'N/A', c.w * 0.4);

    const bw = doc.getTextWidth(estadoText) + 20;
    badge(estadoText, margin + contentW - 16 - bw, c.y + 18);
  }

  // Fechas
  {
    const c = card(92);
    sectionTitle(c.x, c.y, 'Fechas');

    const col1X = c.x;
    const col2X = c.x + c.w * 0.55;

    kvRow(
      col1X,
      c.y + 24,
      'Ocurrencia',
      `${fmtDateLong(ocurrencia)} · ${fmtTime(ocurrencia)}`,
      c.w * 0.5
    );

    kvRow(
      col2X,
      c.y + 24,
      'Generado',
      `${fmtDateLong(generado)} · ${fmtTime(generado)}`,
      c.w * 0.4
    );
  }


  // Características de la línea (catálogo)
  {
    const entry = findLineaCatalogEntry(linea?.numero ?? linea?.nombre ?? null);
    if (entry) {
      // Render en tabla 2 columnas (más organizado que lista)
      const items: Array<[string, string]> = [
        ['Clave enlace', entry.claveEnlace],
        ['Descripción', entry.descripcion],
        ['Área', String(entry.area ?? 'N/A')],
        ['Tensión', entry.tension],
        ['Kms', String(entry.kms ?? 'N/A')],
        ['NC', String(entry.nc ?? 'N/A')],
        ['Conductor', entry.conductor],
        ['Tip. Estruc', entry.tipoEstructura],
        ['# Est', String(entry.numEstructuras ?? 'N/A')],
        ['Año', String(entry.anio ?? 'N/A')],
        ['Comp', entry.comp],
        ['Cve SAP', entry.cveSap],
        ['Brecha', String(entry.brecha ?? 'N/A')],
        ['Conf. cond', entry.confCond],
        ['Pob', entry.pob],
        ['Ent', entry.ent],
      ];

      // altura dinámica aproximada
      const rowH = 36;
      const rows = Math.ceil(items.length / 2);
      const h = Math.min(340, Math.max(140, 68 + rows * rowH));

      const c = card(h);
      sectionTitle(c.x, c.y, 'Características de la línea');

      const gridX = c.x;
      let yy = c.y + 26;
      const colGap = 16;
      const colW = (c.w - colGap) / 2;

      // separador superior sutil
      setDraw(C.line);
      doc.setLineWidth(1);
      doc.line(gridX, yy - 10, gridX + c.w, yy - 10);

      for (let r = 0; r < rows; r++) {
        const left = items[r * 2];
        const right = items[r * 2 + 1];

        const renderCell = (cx: number, cy: number, label: string, value: string) => {
          // label
          doc.setFont('helvetica', 'normal');
          doc.setFontSize(8.2);
          setRGB(C.muted);
          doc.text(label.toUpperCase(), cx, cy);

          // value
          doc.setFont('helvetica', 'normal');
          doc.setFontSize(10.2);
          setRGB(C.text);

          const v = (value ?? '').toString().trim() || 'N/A';
          const lines = doc.splitTextToSize(v, colW);
          doc.text(lines, cx, cy + 14);
        };

        // row background alterno (minimal)
        if (r % 2 === 0) {
          setFill(C.card);
          doc.setFillColor(252, 252, 252);
          doc.rect(margin + 18, yy - 14, contentW - 36, rowH, 'F');
        }

        if (left) renderCell(gridX, yy, left[0], left[1] ?? 'N/A');
        if (right) renderCell(gridX + colW + colGap, yy, right[0], right[1] ?? 'N/A');

        // separador de fila
        setDraw(C.line);
        doc.setLineWidth(1);
        doc.line(gridX, yy + rowH - 14, gridX + c.w, yy + rowH - 14);

        yy += rowH;
      }
    }
  }

  // Descripción
  {
    const text = falla.descripcion?.trim() || 'Sin descripción adicional.';
    doc.setFontSize(10.5);
    const lines = doc.splitTextToSize(text, contentW - 32);
    const h = Math.min(230, Math.max(110, 58 + lines.length * 13));

    const c = card(h);
    sectionTitle(c.x, c.y, 'Descripción');
    setRGB(C.text);
    doc.text(lines, c.x, c.y + 28);
  }

  // Ubicación
  {
    // Si hay Maps URL, el card necesita más altura
    const h = mapsUrl ? 110 : 80;
    const c = card(h);
    sectionTitle(c.x, c.y, 'Ubicación');

    let yy = c.y + 24;
    yy = kvRow(c.x, yy, 'Coordenadas (lat, lon)', coordsText, c.w);

    if (mapsUrl) {
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(8.5);
      setRGB(C.muted);
      doc.text('GOOGLE MAPS', c.x, yy + 2);

      doc.setFont('helvetica', 'normal');
      doc.setFontSize(10.5);
      setRGB(C.brand);
      // Link clickeable (si jsPDF soporta textWithLink)
      if (typeof doc.textWithLink === 'function') {
        doc.textWithLink(mapsUrl, c.x, yy + 18, { url: mapsUrl });
      } else {
        doc.text(mapsUrl, c.x, yy + 18);
      }
      setRGB(C.text);
    }
  }

  // Footer
  {
    setDraw(C.line);
    doc.setLineWidth(1);
    doc.line(margin, pageH - 44, pageW - margin, pageH - 44);

    doc.setFontSize(9);
    setRGB(C.subtext);
    doc.text('Documento generado automáticamente · Uso interno', margin, pageH - 26);

    setRGB(C.muted);
    doc.text(`CFE · Linergy · Folio ${folio}`, pageW - margin, pageH - 26, { align: 'right' });
  }

  doc.save(`reporte-falla-${folio}.pdf`);
}

export function copyFaultText(falla: FaultForReport, linea: LineForReport) {
  const text = generateFaultText(falla, linea);
  navigator.clipboard.writeText(text);
}

function generateFaultText(falla: FaultForReport, linea: LineForReport): string {
  const fecha = new Date(falla.ocurrencia_ts);
  const fechaStr = fecha.toLocaleDateString('es-ES', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
  const horaStr = fecha.toLocaleTimeString('es-ES', {
    hour: '2-digit',
    minute: '2-digit',
  });

  const estadoText =
    ({
      ABIERTA: 'Abierta',
      EN_ATENCION: 'En atención',
      CERRADA: 'Cerrada',
    } as const)[falla.estado] ?? falla.estado;

  // Coordenadas: NO default 0,0
  const geom = parseGeometry(falla.geom ?? null);
  const coords =
    geom?.type === 'Point' &&
    Array.isArray(geom.coordinates) &&
    geom.coordinates.length >= 2
      ? geom.coordinates
      : null;

  const latRaw = coords ? Number(coords[1]) : null; // [lon, lat]
  const lonRaw = coords ? Number(coords[0]) : null;

  const hasValidCoords = isValidLatLon(latRaw, lonRaw);
  const lat = hasValidCoords ? latRaw : null;
  const lon = hasValidCoords ? lonRaw : null;

  const coordsText = hasValidCoords && lat !== null && lon !== null ? `${lat.toFixed(6)}, ${lon.toFixed(6)}` : 'No disponible';
  const mapsUrl = hasValidCoords && lat !== null && lon !== null ? `https://www.google.com/maps?q=${lat},${lon}` : null;

  return `REPORTE DE FALLA - Linergy (CFE)

Folio: ${falla.id.slice(0, 8).toUpperCase()}
Línea: ${linea?.numero || 'N/A'}${linea?.nombre ? ` - ${linea.nombre}` : ''}
${(() => {
  const entry = findLineaCatalogEntry(linea?.numero ?? linea?.nombre ?? null);
  if (!entry) return '';
  const lines = formatLineaCatalogEntryLines(entry);
  return `\nCARACTERÍSTICAS DE LA LÍNEA:\n${lines.join('\n')}\n`;
})()}
Kilómetro: ${Number.isFinite(falla.km) ? falla.km.toFixed(1) : 'N/A'} km
Tipo de falla: ${falla.tipo}
Estado: ${estadoText}

Ocurrencia: ${fechaStr} ${horaStr}

Ubicación:
Coordenadas: ${coordsText}
${mapsUrl ? `Google Maps: ${mapsUrl}` : 'Google Maps: N/A'}

Descripción:
${falla.descripcion || 'Sin descripción adicional'}

ID de falla: ${falla.id}
`;
}