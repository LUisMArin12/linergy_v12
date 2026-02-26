import { useState } from 'react';
import { Info, X } from 'lucide-react';

export default function MapLegend() {
  const [open, setOpen] = useState(false);

  return (
    <div className="absolute bottom-4 left-4 lg:bottom-6 lg:left-6 z-[500] pointer-events-none">
      {/* Toggle button (always visible) */}
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="pointer-events-auto inline-flex items-center justify-center w-11 h-11 rounded-full bg-white border border-[#E5E7EB] shadow-lg hover:bg-[#F7FAF8] transition-colors"
        aria-label={open ? 'Ocultar simbología' : 'Mostrar simbología'}
        title={open ? 'Ocultar simbología' : 'Mostrar simbología'}
      >
        <Info className="w-5 h-5 text-[#6B7280]" />
      </button>

      {open && (
        <div className="pointer-events-auto mt-2 w-[min(78vw,240px)] bg-white rounded-xl shadow-lg border border-[#E5E7EB] overflow-hidden">
          <div className="flex items-center justify-between px-3 py-2 border-b border-[#E5E7EB]">
            <h3 className="text-xs font-semibold text-[#111827]">Simbología</h3>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="p-1.5 rounded-lg hover:bg-[#F7FAF8]"
              aria-label="Cerrar simbología"
            >
              <X className="w-4 h-4 text-[#6B7280]" />
            </button>
          </div>

          <div className="p-3 space-y-3 max-h-[38vh] overflow-y-auto">
            <div className="space-y-2">
              <p className="text-[10px] font-medium text-[#6B7280]">Infraestructura</p>

              <div className="flex items-center gap-2">
                <div className="w-5 h-0.5 bg-[#FF00FF] flex-shrink-0" />
                <span className="text-[11px] text-[#111827]">Línea</span>
              </div>
            </div>

            <div className="space-y-2 pt-2 border-t border-[#E5E7EB]">
              <p className="text-[10px] font-medium text-[#6B7280]">Fallas</p>

              <div className="flex items-center gap-2">
                <div
                  className="w-3.5 h-3.5 rounded-full flex-shrink-0"
                  style={{
                    background: '#EF4444',
                    border: '2px solid white',
                    boxShadow: '0 0 0 1px #E5E7EB',
                  }}
                />
                <span className="text-[11px] text-[#111827]">Abierta</span>
              </div>

              <div className="flex items-center gap-2">
                <div
                  className="w-3.5 h-3.5 rounded-full flex-shrink-0"
                  style={{
                    background: '#F59E0B',
                    border: '2px solid white',
                    boxShadow: '0 0 0 1px #E5E7EB',
                  }}
                />
                <span className="text-[11px] text-[#111827]">En atención</span>
              </div>

              <div className="flex items-center gap-2">
                <div
                  className="w-3.5 h-3.5 rounded-full flex-shrink-0"
                  style={{
                    background: '#10B981',
                    border: '2px solid white',
                    boxShadow: '0 0 0 1px #E5E7EB',
                  }}
                />
                <span className="text-[11px] text-[#111827]">Cerrada (no visible)</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
