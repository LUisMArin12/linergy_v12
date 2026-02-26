// src/pages/MapPage.tsx
import { useState, useEffect, useMemo, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AnimatePresence } from 'framer-motion';
import { useSearchParams, useLocation } from 'react-router-dom';
import MapFilters, { FilterState } from '../components/map/MapFilters';
import ItemsList from '../components/map/ItemsList';
import LeafletMap from '../components/map/LeafletMap';
import MapLegend from '../components/map/MapLegend';
import DetailPanel from '../components/map/DetailPanel';
import FocusToolbar from '../components/map/FocusToolbar';
import WelcomeMessage from '../components/ui/WelcomeMessage';
import { supabase, Linea, Estructura, Falla } from '../lib/supabase';
import { useSearch } from '../contexts/SearchContext';
import { useAuth } from '../contexts/AuthContext';

type DecodedState = {
  filtros?: Partial<FilterState>;
  vista?: { center: [number, number]; zoom: number };
  // Nuevo (preferido)
  faultId?: string;
  lineId?: string;
  // Legacy (compat)
  fallaId?: string;
  lineaId?: string;
};

function safeDecodeState(stateParam: string | null): DecodedState | null {
  if (!stateParam) return null;
  try {
    const normalized = stateParam.replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
    const decodedStr = atob(padded);
    return JSON.parse(decodedStr);
  } catch {
    return null;
  }
}

export default function MapPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { searchQuery } = useSearch();
  const { profile } = useAuth();
  const location = useLocation();
  const [showWelcome, setShowWelcome] = useState(false);

  // ✅ Soporta params nuevos y legacy
  const focusedLineId = searchParams.get('lineId') ?? searchParams.get('lineaId');
  const focusedFaultId = searchParams.get('faultId') ?? searchParams.get('fallaId');

  // ✅ Helpers: actualizar params sin borrar los existentes (ej. state)
  const setParam = useCallback(
    (key: 'lineId' | 'faultId', value?: string | null) => {
      const next = new URLSearchParams(searchParams);

      // Normaliza aliases legacy
      if (key === 'lineId') next.delete('lineaId');
      if (key === 'faultId') next.delete('fallaId');

      if (value === null || value === undefined || value === '') next.delete(key);
      else next.set(key, value);

      setSearchParams(next, { replace: true });
    },
    [setSearchParams, searchParams]
  );

  const deleteParam = useCallback(
    (key: 'lineId' | 'faultId') => {
      const next = new URLSearchParams(searchParams);
      next.delete(key);
      if (key === 'lineId') next.delete('lineaId');
      if (key === 'faultId') next.delete('fallaId');
      setSearchParams(next, { replace: true });
    },
    [setSearchParams, searchParams]
  );

  // ✅ Default: NO visibles hasta que el usuario lo active
  const defaultFilters: FilterState = {
    classifications: [],
    statuses: [],
    showFaults: false,
  };

  const decodedState = useMemo(() => safeDecodeState(searchParams.get('state')), [searchParams]);

  const [showLineResults, setShowLineResults] = useState(true);

  const [filters, setFilters] = useState<FilterState>(() => {
    if (!decodedState) return defaultFilters;
    return { ...defaultFilters, ...(decodedState.filtros ?? {}) };
  });

  const [selectedItem, setSelectedItem] = useState<
    { item: Estructura | Falla; type: 'estructura' | 'falla' } | null
  >(null);

  const [mapView, setMapView] = useState<{ center: [number, number]; zoom: number }>(() => {
    if (decodedState?.vista) return decodedState.vista;
    return { center: [24.0277, -104.6532], zoom: 10 };
  });

  useEffect(() => {
    if (location.state?.fromLogin) {
      setShowWelcome(true);
      window.history.replaceState({}, document.title);
    }
  }, [location]);

  const { data: allLineas = [], error: lineasError, isLoading: lineasLoading } = useQuery({
    queryKey: ['lineas'],
    queryFn: async () => {
      const { data, error } = await supabase.rpc('get_lineas_geojson');
      if (error) {
        console.error('Error fetching lineas:', error);
        throw error;
      }
      return (data || []) as Linea[];
    },
  });

  const { data: allEstructuras = [] } = useQuery({
    queryKey: ['estructuras'],
    queryFn: async () => {
      const { data, error } = await supabase.rpc('get_estructuras_geojson');
      if (error) throw error;
      return (data || []) as Estructura[];
    },
  });

  const { data: allFallas = [] } = useQuery({
    queryKey: ['fallas'],
    queryFn: async () => {
      const { data, error } = await supabase.rpc('get_fallas_geojson');
      if (error) throw error;
      return (data || []) as Falla[];
    },
  });

  useEffect(() => {
    if (lineasError) console.error('Lineas query error:', lineasError);
  }, [lineasError]);

  // ✅ Línea enfocada real (para evitar crash en FocusToolbar)
  const focusedLinea = useMemo(() => {
    if (!focusedLineId) return null;
    return allLineas.find((l) => l.id === focusedLineId) ?? null;
  }, [allLineas, focusedLineId]);

  const lineas = useMemo(() => {
    if (focusedLineId) return allLineas.filter((l) => l.id === focusedLineId);

    let filtered = allLineas;

    if (filters.classifications.length > 0) {
      const uiToDb = { Alta: 'ALTA', Moderada: 'MODERADA', Baja: 'BAJA' } as const;
      const selected = new Set(filters.classifications.map((c) => uiToDb[c]));
      filtered = filtered.filter((l) => selected.has(l.clasificacion));
    }

    return filtered;
  }, [allLineas, focusedLineId, filters.classifications]);

  const estructuras = useMemo(() => {
    if (focusedLineId) return allEstructuras.filter((e) => e.linea_id === focusedLineId);
    if (lineas.length === 0) return [];

    const lineaIds = lineas.map((l) => l.id);
    return allEstructuras.filter((e: Estructura) => lineaIds.includes(e.linea_id));
  }, [allEstructuras, focusedLineId, lineas]);

  const fallas = useMemo(() => {
    if (focusedLineId) return allFallas.filter((f) => f.linea_id === focusedLineId);

    if (!filters.showFaults) return [];
    if (lineas.length === 0) return [];

    const lineaIds = lineas.map((l) => l.id);
    let filtered = allFallas.filter((f: Falla) => lineaIds.includes(f.linea_id));

    if (filters.statuses.length > 0) {
      const uiToDb = { Abierta: 'ABIERTA', 'En atención': 'EN_ATENCION', Cerrada: 'CERRADA' } as const;
      const selected = new Set(filters.statuses.map((s) => uiToDb[s]));
      filtered = filtered.filter((f) => selected.has(f.estado));
    }

    return filtered;
  }, [allFallas, focusedLineId, filters.showFaults, filters.statuses, lineas]);

  const filteredEstructuras = useMemo(() => {
    if (focusedLineId) return estructuras;
    if (!searchQuery) return estructuras;
    const query = searchQuery.toLowerCase();
    return estructuras.filter(
      (e) => e.numero_estructura.toLowerCase().includes(query) || e.km.toString().includes(query)
    );
  }, [estructuras, searchQuery, focusedLineId]);

  const filteredFallas = useMemo(() => {
    if (focusedLineId) return fallas;
    if (!searchQuery) return fallas;
    const query = searchQuery.toLowerCase();
    return fallas.filter(
      (f) =>
        f.tipo.toLowerCase().includes(query) ||
        f.km.toString().includes(query) ||
        (f.descripcion && f.descripcion.toLowerCase().includes(query))
    );
  }, [fallas, searchQuery, focusedLineId]);

  const filteredLineas = useMemo(() => {
    if (focusedLineId) return lineas;
    if (!searchQuery) return lineas;
    const query = searchQuery.toLowerCase();
    return lineas.filter(
      (l) => l.numero.toLowerCase().includes(query) || (l.nombre && l.nombre.toLowerCase().includes(query))
    );
  }, [lineas, searchQuery, focusedLineId]);

  const handleSelectLinea = (lineaId: string) => {
    setParam('lineId', lineaId);
  };

  const handleExitFocus = () => {
    deleteParam('lineId');
  };

  const handleSelectFalla = (f: Falla) => {
    setSelectedItem({ item: f, type: 'falla' });
    setParam('faultId', f.id);
    setFilters((prev) => (prev.showFaults ? prev : { ...prev, showFaults: true }));
  };

  const handleSelectEstructura = (e: Estructura) => {
    setSelectedItem({ item: e, type: 'estructura' });
  };

  const handleClosePanel = () => {
    const wasFalla = selectedItem?.type === 'falla';
    setSelectedItem(null);
    if (wasFalla) deleteParam('faultId');
  };

  // ✅ Rehidratación
  useEffect(() => {
    const state = decodedState;

    const stateFaultId = state?.faultId ?? state?.fallaId ?? null;
    const stateLineId = state?.lineId ?? state?.lineaId ?? null;

    const desiredFaultId = focusedFaultId ?? stateFaultId;
    const desiredLineId = focusedLineId ?? stateLineId;

    if (!focusedLineId && desiredLineId) setParam('lineId', desiredLineId);
    if (!focusedFaultId && desiredFaultId) setParam('faultId', desiredFaultId);

    if (desiredFaultId) {
      const found = allFallas.find((f) => f.id === desiredFaultId);
      if (found) {
        setSelectedItem({ item: found, type: 'falla' });

        if (!focusedLineId && found.linea_id) setParam('lineId', found.linea_id);
        setFilters((prev) => (prev.showFaults ? prev : { ...prev, showFaults: true }));
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [decodedState, focusedFaultId, focusedLineId, allFallas]);

  if (lineasLoading) {
    return (
      <div className="flex items-center justify-center h-[calc(100dvh-8rem)]">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#157A5A] mx-auto mb-4"></div>
          <p className="text-[#6B7280]">Cargando datos...</p>
        </div>
      </div>
    );
  }

  if (lineasError) {
    return (
      <div className="flex items-center justify-center h-[calc(100dvh-8rem)]">
        <div className="text-center">
          <p className="text-red-600 mb-2">Error al cargar los datos</p>
          <p className="text-sm text-[#6B7280]">{String(lineasError)}</p>
        </div>
      </div>
    );
  }

  return (
    <>
      {showWelcome && <WelcomeMessage userName={profile?.email?.split('@')[0]} />}

      {/* ✅ dvh: evita cortes por barras del navegador en iOS/Android */}
      <div className="flex flex-col lg:flex-row gap-4 lg:gap-6 h-[calc(100dvh-8rem)] min-h-0">
      {/* Panel izquierdo */}
      <div className="w-full lg:w-80 flex flex-col min-h-0">
        {/* ✅ Scroll único para: filtros + resultados (sin scroll anidado) */}
        <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain pr-1 [-webkit-overflow-scrolling:touch] space-y-4">
          {/* Filtros arriba */}
          {!focusedLineId && <MapFilters onFiltersChange={setFilters} />}

          {/* Resultados */}
          <ItemsList
            estructuras={filteredEstructuras}
            fallas={filteredFallas}
            lineas={filteredLineas}
            showLineas={showLineResults}
            onToggleLineas={setShowLineResults}
            onSelectEstructura={handleSelectEstructura}
            onSelectFalla={handleSelectFalla}
            onSelectLinea={handleSelectLinea}
          />
        </div>
      </div>

      <div className="flex-1 relative z-0 min-h-[50vh]">
        {/* ✅ Evita crash si lineId no existe / no match */}
        {focusedLineId && focusedLinea ? (
          <FocusToolbar linea={focusedLinea} onExitFocus={handleExitFocus} />
        ) : focusedLineId ? (
          <div className="absolute top-3 left-3 z-[1100] bg-white border border-[#E5E7EB] rounded-lg px-3 py-2 shadow">
            <p className="text-sm text-[#111827]">
              Línea no encontrada para <span className="font-mono">{focusedLineId}</span>
            </p>
            <button className="mt-1 text-sm text-[#157A5A] hover:underline" onClick={handleExitFocus}>
              Salir de foco
            </button>
          </div>
        ) : null}

        <LeafletMap
          estructuras={filteredEstructuras}
          fallas={filteredFallas}
          lineas={filteredLineas}
          focusedLineId={focusedLineId}
          center={mapView.center}
          zoom={mapView.zoom}
          onSelectEstructura={handleSelectEstructura}
          onSelectFalla={handleSelectFalla}
          onSelectLinea={handleSelectLinea}
        />

        <MapLegend />
      </div>

      <AnimatePresence>
        {selectedItem && (
          <DetailPanel
            item={selectedItem.item}
            type={selectedItem.type}
            onClose={handleClosePanel}
            onCenterMap={(lat, lon) => setMapView({ center: [lat, lon], zoom: 16 })}
          />
        )}
      </AnimatePresence>

      {showWelcome && <WelcomeMessage userName={profile?.email?.split('@')[0]} />}
      </div>
    </>
  );
}
