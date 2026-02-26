// src/contexts/MapFocusContext.tsx
import { createContext, useContext, useEffect, useMemo, useState, ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';

interface MapFocusContextType {
  focusedLineId: string | null;
  setFocusedLineId: (lineId: string | null) => void;

  isRegisterFaultOpen: boolean;
  setIsRegisterFaultOpen: (isOpen: boolean) => void;
}

const MapFocusContext = createContext<MapFocusContextType | undefined>(undefined);

export function MapFocusProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams();

  // ✅ Fuente de verdad: URL ?lineId=
  const focusedLineId = useMemo(() => {
    const v = searchParams.get('lineId');
    return v && v.trim() ? v : null;
  }, [searchParams]);

  // ✅ Estado local (solo UI): modal registrar falla
  const [isRegisterFaultOpen, setIsRegisterFaultOpen] = useState(false);

  // ✅ Helper: actualizar params sin borrar otros (state, faultId, etc.)
  const setFocusedLineId = (lineId: string | null) => {
    const next = new URLSearchParams(searchParams);

    if (!lineId) next.delete('lineId');
    else next.set('lineId', lineId);

    setSearchParams(next, { replace: true });
  };

  // ✅ Si ya no hay foco, cerramos el modal para evitar UI colgada
  useEffect(() => {
    if (!focusedLineId) {
      setIsRegisterFaultOpen(false);
    }
  }, [focusedLineId]);

  return (
    <MapFocusContext.Provider
      value={{
        focusedLineId,
        setFocusedLineId,
        isRegisterFaultOpen,
        setIsRegisterFaultOpen,
      }}
    >
      {children}
    </MapFocusContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useMapFocus() {
  const context = useContext(MapFocusContext);
  if (context === undefined) {
    throw new Error('useMapFocus must be used within a MapFocusProvider');
  }
  return context;
}