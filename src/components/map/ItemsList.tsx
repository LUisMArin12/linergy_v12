import { useMemo } from 'react';
import { Route, AlertTriangle, Eye, EyeOff } from 'lucide-react';
import Card from '../ui/Card';
import Badge from '../ui/Badge';
import { Estructura, Falla, Linea } from '../../lib/supabase';

interface ItemsListProps {
  estructuras: Estructura[];
  fallas: Falla[];
  lineas: Linea[];
  showLineas?: boolean;
  onToggleLineas?: (next: boolean) => void;
  onSelectEstructura: (e: Estructura) => void;
  onSelectFalla: (f: Falla) => void;
  onSelectLinea?: (lineaId: string) => void;
}

const estadoLabel: Record<string, string> = {
  ABIERTA: 'Abierta',
  EN_ATENCION: 'En atención',
  CERRADA: 'Cerrada',
};

const fmtKm = (km: unknown) => (Number.isFinite(Number(km)) ? Number(km).toFixed(1) : 'N/A');

export default function ItemsList({
  estructuras,
  fallas,
  lineas,
  showLineas = true,
  onToggleLineas,
  onSelectEstructura,
  onSelectFalla,
  onSelectLinea,
}: ItemsListProps) {
  const hasItems = estructuras.length > 0 || fallas.length > 0 || lineas.length > 0;
  const visibleLineasCount = showLineas ? lineas.length : 0;
  const totalCount = estructuras.length + fallas.length + visibleLineasCount;

  // ✅ evitar recreación en cada render
  const lineasMap = useMemo(() => new Map(lineas.map((l) => [l.id, l])), [lineas]);

  return (
    // ✅ padding inferior para evitar corte en móviles
    <div className="space-y-3 pb-6">
      <div className="flex items-center justify-between gap-2 px-1">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-[#111827]">Resultados</h3>
          <span className="text-xs text-[#6B7280] bg-[#F7FAF8] px-2 py-1 rounded-md">
            {totalCount} {totalCount === 1 ? 'elemento' : 'elementos'}
          </span>
        </div>

        {/* Toggle solo para la sección de líneas (no afecta el mapa) */}
        <button
          type="button"
          onClick={() => onToggleLineas?.(!showLineas)}
          className="inline-flex items-center gap-2 px-2.5 py-2 rounded-lg border border-[#E5E7EB] bg-white hover:bg-[#F7FAF8] transition-colors min-h-[40px]"
          aria-label={showLineas ? 'Ocultar resultados de líneas' : 'Mostrar resultados de líneas'}
          title={showLineas ? 'Ocultar líneas' : 'Mostrar líneas'}
        >
          {showLineas ? <EyeOff className="w-4 h-4 text-[#6B7280]" /> : <Eye className="w-4 h-4 text-[#6B7280]" />}
          <span className="text-xs font-medium text-[#111827]">
            Líneas
            <span className="text-[#6B7280] font-normal"> ({lineas.length})</span>
          </span>
        </button>
      </div>

      {!hasItems ? (
        <div className="bg-white rounded-xl border border-[#E5E7EB] p-8 text-center">
          <div className="w-12 h-12 bg-[#F7FAF8] rounded-full flex items-center justify-center mx-auto mb-3">
            <Route className="w-6 h-6 text-[#6B7280]" />
          </div>
          <p className="text-sm text-[#6B7280]">No hay resultados con los filtros aplicados</p>
        </div>
      ) : (
        <div className="space-y-3">
          {showLineas ? (
            lineas.map((linea) => (
              <Card
                key={linea.id}
                hover
                clickable
                onClick={() => onSelectLinea?.(linea.id)}
                className="p-4 cursor-pointer"
              >
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 bg-[#FFE5FF] rounded-lg flex items-center justify-center flex-shrink-0">
                    <Route className="w-4 h-4 text-[#FF00FF]" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <p className="font-semibold text-[#111827] truncate">{linea.numero}</p>
                      <Badge variant="classification" classification={linea.clasificacion}>
                        {linea.clasificacion}
                      </Badge>
                    </div>

                    {linea.nombre && <p className="text-sm text-[#6B7280] truncate">{linea.nombre}</p>}

                    {linea.km_inicio !== null && linea.km_fin !== null && (
                      <p className="text-xs text-[#6B7280] mt-1">
                        {fmtKm(linea.km_inicio)} - {fmtKm(linea.km_fin)} km
                      </p>
                    )}
                  </div>
                </div>
              </Card>
            ))
          ) : (
            <div className="bg-white rounded-xl border border-[#E5E7EB] p-3">
              <p className="text-xs text-[#6B7280]">Líneas ocultas para reducir carga visual.</p>
            </div>
          )}

          {estructuras.map((estructura) => {
            const linea = lineasMap.get(estructura.linea_id);
            return (
              <Card
                key={estructura.id}
                hover
                clickable
                onClick={() => onSelectEstructura(estructura)}
                className="p-4 cursor-pointer"
              >
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 bg-[#DDF3EA] rounded-lg flex items-center justify-center flex-shrink-0">
                    <Route className="w-4 h-4 text-[#157A5A]" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <p className="font-semibold text-[#111827] truncate">{linea?.numero || 'N/A'}</p>
                      {linea && (
                        <Badge variant="classification" classification={linea.clasificacion}>
                          {linea.clasificacion}
                        </Badge>
                      )}
                    </div>
                    <p className="text-sm text-[#6B7280]">
                      Estructura {estructura.numero_estructura} • Km {fmtKm(estructura.km)}
                    </p>
                  </div>
                </div>
              </Card>
            );
          })}

          {fallas.map((falla) => {
            const linea = lineasMap.get(falla.linea_id);

            const fecha = new Date(falla.ocurrencia_ts);
            const fechaStr = Number.isNaN(fecha.getTime()) ? 'N/A' : fecha.toLocaleDateString('es-ES');
            const horaStr = Number.isNaN(fecha.getTime())
              ? ''
              : fecha.toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' });

            return (
              <Card
                key={falla.id}
                hover
                clickable
                onClick={() => onSelectFalla(falla)}
                className="p-4 cursor-pointer"
              >
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 bg-red-50 rounded-lg flex items-center justify-center flex-shrink-0">
                    <AlertTriangle className="w-4 h-4 text-red-600" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <p className="font-semibold text-[#111827] truncate">{falla.tipo}</p>
                      <Badge variant="status" status={falla.estado}>
                        {estadoLabel[falla.estado] || falla.estado}
                      </Badge>
                    </div>

                    <p className="text-sm text-[#6B7280]">
                      {linea?.numero || 'N/A'} • Km {fmtKm(falla.km)}
                    </p>

                    <p className="text-xs text-[#6B7280] mt-1">
                      {fechaStr} {horaStr}
                    </p>
                  </div>
                </div>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
