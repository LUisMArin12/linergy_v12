import { useState } from 'react';
import { Copy, Download, ExternalLink, Navigation, CheckCircle } from 'lucide-react';
import Modal from '../ui/Modal';
import Button from '../ui/Button';
import Toast from '../ui/Toast';
import { generateFaultPDF, type FaultForReport } from '../../lib/reportUtils';

interface FaultReportModalProps {
  isOpen: boolean;
  onClose: () => void;
  faultData: {
    lat: number | null;
    lon: number | null;
    hasValidCoords?: boolean;

    fallaId: string;
    lineaNumero: string;
    lineaNombre: string;
    km: number;
    tipo: string;
    fecha: string;
    hora: string;
    descripcion: string;
    estado: string;
  };
}

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

export default function FaultReportModal({ isOpen, onClose, faultData }: FaultReportModalProps) {
  const [showCopiedToast, setShowCopiedToast] = useState(false);

  const hasValidCoords =
    typeof faultData.hasValidCoords === 'boolean'
      ? faultData.hasValidCoords
      : isValidLatLon(faultData.lat, faultData.lon);

  const coordsText =
    hasValidCoords && faultData.lat !== null && faultData.lon !== null
      ? `${faultData.lat.toFixed(6)}, ${faultData.lon.toFixed(6)}`
      : 'N/A';

  const googleMapsUrl =
    hasValidCoords && faultData.lat !== null && faultData.lon !== null
      ? `https://www.google.com/maps?q=${faultData.lat},${faultData.lon}`
      : null;

  const googleMapsNavUrl =
    hasValidCoords && faultData.lat !== null && faultData.lon !== null
      ? `https://www.google.com/maps/dir/?api=1&destination=${faultData.lat},${faultData.lon}`
      : null;

  const generateReportText = () => {
    const now = new Date();
    const fechaGeneracion = now.toLocaleDateString('es-MX', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
    const horaGeneracion = now.toLocaleTimeString('es-MX', {
      hour: '2-digit',
      minute: '2-digit',
    });

    const folio = faultData.fallaId.slice(0, 8).toUpperCase();

    const ocurrenciaDate = new Date(faultData.fecha);
    const fechaOcurrencia = ocurrenciaDate.toLocaleDateString('es-MX', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });

    const header = `LINERGY – REPORTE DE FALLA
────────────────────────────────────────────`;

    const rows: Array<[string, string, string]> = [
      ['GENERAL', 'Sistema', 'Linergy'],
      ['GENERAL', 'Folio', folio],
      ['GENERAL', 'Estado', String(faultData.estado || '').toUpperCase()],
      ['GENERAL', 'Generado', `${fechaGeneracion} · ${horaGeneracion}`],

      ['LÍNEA', 'Línea', `${faultData.lineaNumero}${faultData.lineaNombre ? ` — ${faultData.lineaNombre}` : ''}`],
      ['LÍNEA', 'Kilómetro', `${Number.isFinite(faultData.km) ? faultData.km.toFixed(1) : faultData.km} km`],

      ['FALLA', 'Tipo', faultData.tipo || 'N/A'],
      ['FALLA', 'Fecha', fechaOcurrencia],
      ['FALLA', 'Hora', faultData.hora || 'N/A'],
      ['FALLA', 'Descripción', faultData.descripcion?.trim() ? faultData.descripcion.trim() : 'Sin descripción adicional'],

      ['UBICACIÓN', 'Coordenadas', hasValidCoords ? coordsText : 'N/A'],
      ['UBICACIÓN', 'Google Maps', googleMapsUrl ?? 'N/A'],
    ];

    const col1 = 10; // SECCIÓN
    const col2 = 16; // CAMPO

    const pad = (s: string, n: number) => (s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length));
    const sep = `${'─'.repeat(col1)}┼${'─'.repeat(col2)}┼${'─'.repeat(36)}`;

    const lines = [
      header,
      '',
      `${pad('SECCIÓN', col1)}│${pad('CAMPO', col2)}│ VALOR`,
      sep,
      ...rows.map(([sec, campo, val]) => `${pad(sec, col1)}│${pad(campo, col2)}│ ${val}`),
    ];

    return lines.join('\n');
};

  const handleCopyReport = () => {
    const reportText = generateReportText();
    navigator.clipboard.writeText(reportText);
    setShowCopiedToast(true);
  };

  const normalizeEstadoToDbEnum = (
    estado: string
  ): 'ABIERTA' | 'EN_ATENCION' | 'CERRADA' => {
    if (estado === 'ABIERTA' || estado === 'EN_ATENCION' || estado === 'CERRADA') return estado;
    if (estado === 'Abierta') return 'ABIERTA';
    if (estado === 'En atención') return 'EN_ATENCION';
    if (estado === 'Cerrada') return 'CERRADA';
    return 'ABIERTA';
  };

  const handleExportPDF = () => {
    // Adaptamos el shape del modal al esperado por generateFaultPDF(falla, linea)
    const falla: FaultForReport = {
      id: faultData.fallaId,
      ocurrencia_ts: new Date(`${faultData.fecha}T${faultData.hora}`).toISOString(),
      km: faultData.km,
      tipo: faultData.tipo,
      descripcion: faultData.descripcion,
      estado: normalizeEstadoToDbEnum(faultData.estado),
      geom:
        hasValidCoords && faultData.lat !== null && faultData.lon !== null
          ? `POINT(${faultData.lon} ${faultData.lat})`
          : null,
    };

    const linea = {
      numero: faultData.lineaNumero,
      nombre: faultData.lineaNombre,
    };

    generateFaultPDF(falla, linea);
  };

  const openMaps = () => {
    if (!googleMapsUrl) return;
    window.open(googleMapsUrl, '_blank');
  };

  const openNav = () => {
    if (!googleMapsNavUrl) return;
    window.open(googleMapsNavUrl, '_blank');
  };

  return (
    <>
      <Modal isOpen={isOpen} onClose={onClose} title="Reporte de Falla" size="lg">
        <div className="space-y-6">
          <div className="bg-green-50 border border-green-200 rounded-lg p-4 text-center">
            <div className="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-3">
              <CheckCircle className="w-6 h-6 text-green-600" />
            </div>
            <p className="font-semibold text-green-800 mb-1">Falla registrada correctamente</p>
            <p className="text-xs text-green-600 font-mono">ID: {faultData.fallaId}</p>
          </div>

          <div className="bg-white border border-[#E5E7EB] rounded-lg p-4">
            <h3 className="font-semibold text-[#111827] mb-3">Resumen del Reporte</h3>

            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-[#6B7280]">Línea:</span>
                <span className="font-medium text-[#111827]">
                  {faultData.lineaNumero}
                  {faultData.lineaNombre ? ` - ${faultData.lineaNombre}` : ''}
                </span>
              </div>

              <div className="flex justify-between">
                <span className="text-[#6B7280]">Kilómetro:</span>
                <span className="font-medium text-[#111827]">{faultData.km} km</span>
              </div>

              <div className="flex justify-between">
                <span className="text-[#6B7280]">Tipo:</span>
                <span className="font-medium text-[#111827]">{faultData.tipo}</span>
              </div>

              <div className="flex justify-between">
                <span className="text-[#6B7280]">Estado:</span>
                <span className="px-2 py-1 bg-red-100 text-red-700 text-xs font-medium rounded">
                  {faultData.estado}
                </span>
              </div>

              <div className="flex justify-between">
                <span className="text-[#6B7280]">Coordenadas:</span>
                <span className="font-mono text-xs text-[#111827]">{coordsText}</span>
              </div>
            </div>

            {!hasValidCoords && (
              <p className="mt-3 text-xs text-[#6B7280]">
                Esta falla no tiene coordenadas válidas registradas. Las acciones de navegación están deshabilitadas.
              </p>
            )}
          </div>

          <div className="space-y-3">
            <Button
              variant="primary"
              icon={<Copy className="w-4 h-4" />}
              onClick={handleCopyReport}
              className="w-full"
            >
              Copiar reporte de texto
            </Button>

            <Button
              variant="secondary"
              icon={<Download className="w-4 h-4" />}
              onClick={handleExportPDF}
              className="w-full"
            >
              Exportar reporte (PDF)
            </Button>

            <div className="grid grid-cols-2 gap-3">
              <Button
                variant="secondary"
                icon={<ExternalLink className="w-4 h-4" />}
                onClick={openMaps}
                disabled={!hasValidCoords}
              >
                Ver en Maps
              </Button>
              <Button
                variant="secondary"
                icon={<Navigation className="w-4 h-4" />}
                onClick={openNav}
                disabled={!hasValidCoords}
              >
                Navegar
              </Button>
            </div>

            <Button variant="secondary" onClick={onClose} className="w-full mt-4">
              Cerrar
            </Button>
          </div>
        </div>
      </Modal>

      <Toast
        message="Reporte copiado al portapapeles"
        type="success"
        isVisible={showCopiedToast}
        onClose={() => setShowCopiedToast(false)}
      />
    </>
  );
}