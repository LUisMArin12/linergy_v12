import { MapPin, AlertTriangle } from 'lucide-react';
import { Structure, Fault } from '../../types';

interface MapPlaceholderProps {
  structures: Structure[];
  faults: Fault[];
  onSelectStructure: (s: Structure) => void;
  onSelectFault: (f: Fault) => void;
}

export default function MapPlaceholder({ structures, faults, onSelectStructure, onSelectFault }: MapPlaceholderProps) {
  return (
    <div className="bg-white rounded-xl border border-[#E5E7EB] p-4 h-full relative overflow-hidden">
      <div className="absolute top-4 right-4 bg-white rounded-lg border border-[#E5E7EB] shadow-lg p-3 z-10">
        <p className="text-xs font-semibold text-[#111827] mb-2">Leyenda</p>
        <div className="space-y-1.5">
          <div className="flex items-center gap-2">
            <MapPin className="w-4 h-4 text-[#157A5A]" />
            <span className="text-xs text-[#6B7280]">Estructura</span>
          </div>
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-4 h-4 text-red-600" />
            <span className="text-xs text-[#6B7280]">Falla</span>
          </div>
        </div>
      </div>

      {/* Placeholder visual (no datos reales). */}
      <svg className="w-full h-full" viewBox="0 0 800 600" aria-hidden="true">
        <defs>
          <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
            <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
          </pattern>
        </defs>

        <rect width="800" height="600" fill="url(#grid)" />

        <polyline points="100,150 200,180 350,120 500,200 650,160" fill="none" stroke="#157A5A" strokeWidth="3" opacity="0.6" />
        <polyline points="150,350 280,320 420,380 580,340" fill="none" stroke="#157A5A" strokeWidth="3" opacity="0.6" />

        {structures.slice(0, 5).map((s, idx) => (
          <g key={s.id} onClick={() => onSelectStructure(s)} style={{ cursor: 'pointer' }}>
            <circle cx={180 + idx * 110} cy={160 + (idx % 2) * 70} r="7" fill="#10B981" stroke="white" strokeWidth="2" />
          </g>
        ))}

        {faults.slice(0, 5).map((f, idx) => (
          <g key={f.id} onClick={() => onSelectFault(f)} style={{ cursor: 'pointer' }}>
            <circle cx={220 + idx * 100} cy={360 + (idx % 2) * 60} r="7" fill="#EF4444" stroke="white" strokeWidth="2" />
          </g>
        ))}
      </svg>
    </div>
  );
}
