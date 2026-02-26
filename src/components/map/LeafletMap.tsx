import { useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet.markercluster';
import { Estructura, Falla, Linea, parseGeometry } from '../../lib/supabase';

interface LeafletMapProps {
  estructuras: Estructura[];
  fallas: Falla[];
  lineas: Linea[];
  focusedLineId?: string | null;
  onSelectEstructura?: (estructura: Estructura) => void;
  onSelectFalla?: (falla: Falla) => void;
  onSelectLinea?: (lineaId: string) => void;
  center?: [number, number];
  zoom?: number;
}

const defaultCenter: [number, number] = [24.0277, -104.6532];

const estadoLabel: Record<string, string> = {
  ABIERTA: 'Abierta',
  EN_ATENCION: 'En atención',
  CERRADA: 'Cerrada',
};

// leaflet.markercluster no tipa bien markerClusterGroup en ESM; usamos un wrapper tipado mínimo.
type ClusterLike = { getChildCount(): number };
type ClusterGroupLike = L.LayerGroup & { addLayer(layer: L.Layer): void; getLayers(): L.Layer[] };
const markerClusterGroup = (options: unknown): ClusterGroupLike =>
  (L as unknown as { markerClusterGroup: (opts: unknown) => ClusterGroupLike }).markerClusterGroup(options);

export default function LeafletMap({
  estructuras,
  fallas,
  lineas,
  focusedLineId,
  onSelectEstructura,
  onSelectFalla,
  onSelectLinea,
  center = defaultCenter,
  zoom = 6,
}: LeafletMapProps) {
  const mapRef = useRef<L.Map | null>(null);
  const mapContainerRef = useRef<HTMLDivElement>(null);

  // ✅ Overlay container: aquí metemos todo lo que NO es tileLayer
  const overlayGroupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    const map = L.map(mapContainerRef.current, {
      center,
      zoom,
      zoomControl: true,
    });

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap contributors',
      maxZoom: 19,
    }).addTo(map);

    mapRef.current = map;

    // ✅ una sola capa “contenedora” para overlays
    overlayGroupRef.current = L.layerGroup().addTo(map);

    return () => {
      overlayGroupRef.current?.clearLayers();
      overlayGroupRef.current = null;

      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ✅ Re-render de overlays sin duplicados
  useEffect(() => {
    const map = mapRef.current;
    const overlay = overlayGroupRef.current;
    if (!map || !overlay) return;

    // 🔥 FIX: limpia TODO lo agregado en el render anterior
    overlay.clearLayers();

    // Si por alguna razón upstream no filtra, aquí aplicamos foco como “seguro”
    const filteredLineas = focusedLineId ? lineas.filter((l) => l.id === focusedLineId) : lineas;
    const filteredEstructuras = focusedLineId ? estructuras.filter((e) => e.linea_id === focusedLineId) : estructuras;
    const filteredFallas = focusedLineId ? fallas.filter((f) => f.linea_id === focusedLineId) : fallas;

    // --- LINEAS ---
    const linesLayer = L.layerGroup();

    let focusedBounds: L.LatLngBounds | null = null;

    const addPolyline = (lineaId: string, numero: string, latLngs: [number, number][]) => {
      const polyline = L.polyline(latLngs, {
        color: '#FF00FF',
        weight: 4,
        opacity: 0.8,
      })
        .bindPopup(`<strong>Línea ${numero}</strong>`)
        .on('click', () => onSelectLinea?.(lineaId));

      linesLayer.addLayer(polyline);

      if (focusedLineId && lineaId === focusedLineId) {
        focusedBounds = polyline.getBounds();
      }
    };

    filteredLineas.forEach((linea) => {
      if (!linea.geom) return;

      const geom = parseGeometry(linea.geom);
      if (!geom) return;

      if (geom.type === 'LineString') {
        const latLngs = geom.coordinates.map((c) => [c[1], c[0]] as [number, number]);
        addPolyline(linea.id, linea.numero, latLngs);
      }

      if (geom.type === 'MultiLineString') {
        geom.coordinates.forEach((segment) => {
          const latLngs = segment.map((c) => [c[1], c[0]] as [number, number]);
          addPolyline(linea.id, linea.numero, latLngs);
        });
      }
    });

    overlay.addLayer(linesLayer);

    // Fit bounds solo si hay foco y bounds disponibles
    if (focusedLineBoundsValid(focusedBounds)) {
      map.fitBounds(focusedBounds!, { padding: [50, 50] });
    }

    // --- CLUSTER ESTRUCTURAS ---
    const estructurasCluster = markerClusterGroup({
      showCoverageOnHover: false,
      maxClusterRadius: 50,
      iconCreateFunction: (cluster: ClusterLike) => {
        const count = cluster.getChildCount();
        return L.divIcon({
          html: `<div style="background:#3B82F6;color:white;border-radius:50%;width:32px;height:32px;display:flex;align-items:center;justify-content:center;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);font-size:12px;font-weight:bold;">${count}</div>`,
          className: 'marker-cluster-custom',
          iconSize: [32, 32],
        });
      },
    });

    filteredEstructuras.forEach((estructura) => {
      const geom = parseGeometry(estructura.geom);
      if (!geom || geom.type !== 'Point') return;

      const [lon, lat] = geom.coordinates;

      const marker = L.circleMarker([lat, lon], {
        radius: 5,
        fillColor: '#3B82F6',
        color: '#ffffff',
        weight: 2,
        opacity: 1,
        fillOpacity: 0.8,
      })
        .bindPopup(
          `<div>
            <strong>${estructura.numero_estructura}</strong><br/>
            KM: ${Number.isFinite(estructura.km) ? estructura.km.toFixed(1) : 'N/A'}
          </div>`
        )
        .on('click', () => onSelectEstructura?.(estructura));

      estructurasCluster.addLayer(marker);
    });

    if (estructurasCluster.getLayers().length > 0) {
      overlay.addLayer(estructurasCluster);
    }

    // --- CLUSTER FALLAS ---
    const fallaColorMap: Record<string, string> = {
      ABIERTA: '#EF4444',
      EN_ATENCION: '#F59E0B',
      CERRADA: '#10B981',
    };

    const fallasCluster = markerClusterGroup({
      showCoverageOnHover: false,
      maxClusterRadius: 50,
      iconCreateFunction: (cluster: ClusterLike) => {
        const count = cluster.getChildCount();
        return L.divIcon({
          html: `<div style="background:#EF4444;color:white;border-radius:50%;width:32px;height:32px;display:flex;align-items:center;justify-content:center;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);font-size:12px;font-weight:bold;">${count}</div>`,
          className: 'marker-cluster-custom',
          iconSize: [32, 32],
        });
      },
    });

    filteredFallas.forEach((falla) => {
      const geom = parseGeometry(falla.geom);
      if (!geom || geom.type !== 'Point') return;

      const [lon, lat] = geom.coordinates;
      const color = fallaColorMap[falla.estado] || '#EF4444';

      const marker = L.circleMarker([lat, lon], {
        radius: 7,
        fillColor: color,
        color: '#ffffff',
        weight: 2,
        opacity: 1,
        fillOpacity: 0.9,
      })
        .bindPopup(
          `<div>
            <strong>${falla.tipo}</strong><br/>
            Estado: ${estadoLabel[falla.estado] || falla.estado}<br/>
            KM: ${Number.isFinite(falla.km) ? falla.km.toFixed(1) : 'N/A'}
          </div>`
        )
        .on('click', () => onSelectFalla?.(falla));

      fallasCluster.addLayer(marker);
    });

    if (fallasCluster.getLayers().length > 0) {
      overlay.addLayer(fallasCluster);
    }
  }, [estructuras, fallas, lineas, focusedLineId, onSelectEstructura, onSelectFalla, onSelectLinea]);

  // ✅ Center/zoom externos (ej. “centrar en mapa” desde panel)
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.setView(center, zoom);
  }, [center, zoom]);

  return (
    <div className="relative w-full h-full">
      <div ref={mapContainerRef} className="w-full h-full rounded-lg" />
    </div>
  );
}

function focusedLineBoundsValid(b: L.LatLngBounds | null): boolean {
  if (!b) return false;
  try {
    return b.isValid();
  } catch {
    return false;
  }
}