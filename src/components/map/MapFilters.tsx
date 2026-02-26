// src/components/map/MapFilters.tsx
import { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import Chip from '../ui/Chip';
import Badge from '../ui/Badge';
import { Filter, ChevronDown, X } from 'lucide-react';
import { Classification, FaultStatus } from '../../types';

export interface FilterState {
  classifications: Classification[];
  statuses: FaultStatus[];
  showFaults: boolean;
}

interface MapFiltersProps {
  onFiltersChange: (filters: FilterState) => void;
}

export default function MapFilters({ onFiltersChange }: MapFiltersProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  // ✅ Default: NO visibles hasta que el usuario lo active
  const [filters, setFilters] = useState<FilterState>({
    classifications: [],
    statuses: [],
    showFaults: false,
  });

  // ✅ Notificar al padre DESPUÉS del render (evita warning)
  useEffect(() => {
    onFiltersChange(filters);
  }, [filters, onFiltersChange]);

  const updateFilters = (update: Partial<FilterState>) => {
    setFilters((prev) => ({ ...prev, ...update }));
  };

  const toggleClassification = (c: Classification) => {
    setFilters((prev) => ({
      ...prev,
      classifications: prev.classifications.includes(c)
        ? prev.classifications.filter((x) => x !== c)
        : [...prev.classifications, c],
    }));
  };

  const toggleStatus = (s: FaultStatus) => {
    setFilters((prev) => ({
      ...prev,
      statuses: prev.statuses.includes(s) ? prev.statuses.filter((x) => x !== s) : [...prev.statuses, s],
    }));
  };

  const activeFiltersCount = filters.classifications.length + filters.statuses.length;

  const clearAllFilters = () => {
    setFilters({
      classifications: [],
      statuses: [],
      showFaults: false,
    });
  };

  return (
    <div className="bg-white rounded-xl border border-[#E5E7EB] shadow-sm overflow-hidden">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full p-4 flex items-center justify-between hover:bg-[#F7FAF8] transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-[#DDF3EA] rounded-lg flex items-center justify-center">
            <Filter className="w-4 h-4 text-[#157A5A]" />
          </div>

          <div className="text-left">
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-[#111827]">Filtros</span>
              {activeFiltersCount > 0 && (
                <Badge className="bg-[#157A5A] text-white border-[#157A5A]">{activeFiltersCount}</Badge>
              )}
            </div>

            <p className="text-xs text-[#6B7280]">{isExpanded ? 'Contraer filtros' : 'Expandir filtros'}</p>
          </div>
        </div>

        <ChevronDown className={`w-5 h-5 text-[#6B7280] transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
      </button>

      <AnimatePresence>
        {isExpanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="border-t border-[#E5E7EB] overflow-hidden"
          >
            {/* ✅ SIN scroll interno: el scroll lo hace el panel izquierdo (MapPage) */}
            <div className="p-4 pb-4 space-y-4">
              {activeFiltersCount > 0 && (
                <button
                  type="button"
                  onClick={clearAllFilters}
                  className="text-xs font-medium text-red-600 hover:text-red-700 flex items-center gap-1"
                >
                  <X className="w-3 h-3" />
                  Limpiar todos los filtros
                </button>
              )}

              <div>
                <label className="text-xs font-semibold text-[#6B7280] uppercase tracking-wide mb-2 block">
                  Clasificación
                </label>
                <div className="flex flex-wrap gap-2">
                  {(['Alta', 'Moderada', 'Baja'] as Classification[]).map((c) => (
                    <Chip
                      key={c}
                      selected={filters.classifications.includes(c)}
                      onClick={() => toggleClassification(c)}
                      className="text-xs"
                    >
                      {c}
                    </Chip>
                  ))}
                </div>
              </div>

              <div>
                <label className="text-xs font-semibold text-[#6B7280] uppercase tracking-wide mb-2 block">
                  Estado
                </label>
                <div className="flex flex-wrap gap-2">
                  {(['Abierta', 'En atención', 'Cerrada'] as FaultStatus[]).map((s) => (
                    <Chip
                      key={s}
                      selected={filters.statuses.includes(s)}
                      onClick={() => toggleStatus(s)}
                      className="text-xs"
                    >
                      {s}
                    </Chip>
                  ))}
                </div>
              </div>

              <div className="pt-3 border-t border-[#E5E7EB]">
                <label className="text-xs font-semibold text-[#6B7280] uppercase tracking-wide mb-3 block">
                  Visibilidad
                </label>

                <div className="space-y-2">
                  <label className="flex items-center gap-2 cursor-pointer group">
                    <input
                      type="checkbox"
                      checked={filters.showFaults}
                      onChange={(e) => updateFilters({ showFaults: e.target.checked })}
                      className="w-4 h-4 text-[#157A5A] rounded focus:ring-[#157A5A]"
                    />
                    <span className="text-sm text-[#111827] group-hover:text-[#157A5A] transition-colors">
                      Fallas
                    </span>
                  </label>
                </div>
              </div>

              {/* ✅ pequeño colchón para móviles */}
              <div className="h-2" />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
