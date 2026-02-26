import { useState, useEffect, useCallback, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { MapPin, FileText, AlertCircle } from 'lucide-react';
import Modal from '../ui/Modal';
import Input from '../ui/Input';
import SearchableSelect from '../ui/SearchableSelect';
import Button from '../ui/Button';
import { supabase, computeFaultLocation, callRpc } from '../../lib/supabase';
import { useToast } from '../../contexts/ToastContext';
import { useMapFocus } from '../../contexts/MapFocusContext';
import { useAuth } from '../../contexts/AuthContext';
import FaultReportModal from './FaultReportModal';

interface FaultFormData {
  lineaId: string;
  km: number | null;
  tipo: string;
  fecha: string;
  hora: string;
  descripcion: string;
}

interface FaultResult {
  lat: number;
  lon: number;
  fallaId: string;
  lineaNumero: string;
  lineaNombre: string;
  km: number;
  tipo: string;
  fecha: string;
  hora: string;
  descripcion: string;
  estado: string;
}

type LineaRow = {
  id: string;
  numero: string;
  nombre: string | null;
};

export default function RegisterFaultModal() {
  const queryClient = useQueryClient();
  const { showToast } = useToast();
  const { isRegisterFaultOpen, setIsRegisterFaultOpen } = useMapFocus();
  const { isAdmin } = useAuth();

  const [searchParams, setSearchParams] = useSearchParams();
  const focusedLineId = searchParams.get('lineId'); // foco por URL (fuente de verdad)

  const [showReportModal, setShowReportModal] = useState(false);
  const [result, setResult] = useState<FaultResult | null>(null);

  const buildInitialFormData = useCallback((lineaId?: string | null): FaultFormData => {
    const now = new Date();
    return {
      lineaId: lineaId ?? '',
      km: null,
      tipo: '',
      fecha: now.toISOString().split('T')[0],
      hora: now.toTimeString().slice(0, 5),
      descripcion: '',
    };
  }, []);

  const [formData, setFormData] = useState<FaultFormData>(() => buildInitialFormData(focusedLineId));

  // Helper: actualizar params sin borrar los existentes
  const setParam = useCallback(
    (key: string, value?: string | null) => {
      const next = new URLSearchParams(window.location.search);
      if (value === null || value === undefined || value === '') next.delete(key);
      else next.set(key, value);
      setSearchParams(next, { replace: true });
    },
    [setSearchParams]
  );

  // Si hay foco: solo traemos esa línea. Si no: todas para el select.
  const { data: lineas = [], isFetching: lineasLoading } = useQuery({
    queryKey: ['lineas_select_modal', focusedLineId],
    queryFn: async () => {
      const q = supabase.from('lineas').select('id,numero,nombre').order('numero');

      const { data, error } = focusedLineId
        ? await q.eq('id', focusedLineId)
        : await q;

      if (error) throw error;
      return (data ?? []) as LineaRow[];
    },
    enabled: isRegisterFaultOpen,
  });

  const focusedLinea = useMemo(() => {
    if (!focusedLineId) return null;
    return lineas.find((l) => l.id === focusedLineId) ?? null;
  }, [lineas, focusedLineId]);

  const createFallaMutation = useMutation({
    mutationFn: async (data: FaultFormData) => {
      if (!data.lineaId) throw new Error('Selecciona una línea para continuar');

      if (lineas.length === 0) {
        throw new Error('No hay líneas disponibles. Importa líneas desde el panel de administración');
      }

      const km = data.km === null ? null : Number(data.km);
      if (km === null || !Number.isFinite(km) || km < 0) {
        throw new Error('Ingresa un kilómetro válido (debe ser mayor o igual a 0)');
      }

      const tipo = data.tipo.trim();
      if (!tipo) throw new Error('Indica el tipo de falla');

      let location: { lat: number; lon: number };
      try {
        location = await computeFaultLocation(data.lineaId, km);
      } catch (e) {
        const msg = e instanceof Error ? e.message : 'Error al calcular ubicación';
        throw new Error(msg);
      }

      const lat = Number(location.lat);
      const lon = Number(location.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        throw new Error('No se pudo calcular la ubicación. Verifica el kilómetro ingresado');
      }

      const geomWkt = `POINT(${lon} ${lat})`;
      const ocurrenciaTs = new Date(`${data.fecha}T${data.hora}`).toISOString();

      const fallaArray = await callRpc<Array<{ id: string; linea_id: string; km: number; tipo: string; descripcion: string | null; estado: string; ocurrencia_ts: string }>>('insert_falla_with_wkt', {
        p_linea_id: data.lineaId,
        p_km: km,
        p_tipo: tipo,
        p_descripcion: data.descripcion?.trim() ? data.descripcion.trim() : null,
        p_ocurrencia_ts: ocurrenciaTs,
        p_estado: 'ABIERTA',
        p_geom_wkt: geomWkt,
      });

      if (!fallaArray || fallaArray.length === 0) {
        throw new Error('Error al registrar la falla. Intenta nuevamente');
      }

      // ✅ Garantiza info de línea aunque no haya cargado el select
      const { data: linea, error: lineaErr } = await supabase
        .from('lineas')
        .select('id,numero,nombre')
        .eq('id', data.lineaId)
        .maybeSingle();

      if (lineaErr) throw lineaErr;

      return { falla: fallaArray[0], location: { lat, lon }, linea, input: { ...data, km } };
    },
    onSuccess: (data) => {
      const faultResult: FaultResult = {
        lat: data.location.lat,
        lon: data.location.lon,
        fallaId: data.falla.id,
        lineaNumero: data.linea?.numero || '',
        lineaNombre: data.linea?.nombre || '',
        km: data.input.km as number,
        tipo: data.input.tipo,
        fecha: data.input.fecha,
        hora: data.input.hora,
        descripcion: data.input.descripcion,
        estado: 'ABIERTA',
      };

      setResult(faultResult);

      // ✅ URL shareable: conserva params y agrega/actualiza lineId + faultId
      setParam('lineId', data.input.lineaId);
      setParam('faultId', data.falla.id);

      queryClient.invalidateQueries({ queryKey: ['fallas'] });
      showToast('Falla registrada correctamente', 'success');

      // limpiar state del modal
      setFormData(buildInitialFormData(focusedLineId));
      createFallaMutation.reset();

      setIsRegisterFaultOpen(false);
      setShowReportModal(true);
    },
    onError: (err) => {
      const msg = err instanceof Error ? err.message : 'Error al registrar la falla';
      showToast(msg, 'error');
    },
  });

  // Cada vez que el modal se abre: resetea el formulario (y respeta foco actual)
  useEffect(() => {
    if (isRegisterFaultOpen) {
      setFormData(buildInitialFormData(focusedLineId));
      createFallaMutation.reset();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isRegisterFaultOpen, focusedLineId, buildInitialFormData]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    const kmValid = formData.km !== null && Number.isFinite(Number(formData.km)) && Number(formData.km) >= 0;
    const tipoValid = formData.tipo.trim().length > 0;

    if (!formData.lineaId || !kmValid || !tipoValid) return;

    createFallaMutation.mutate(formData);
  };

  const handleClose = () => {
    setFormData(buildInitialFormData(focusedLineId));
    createFallaMutation.reset();
    setIsRegisterFaultOpen(false);
  };

  const handleReportClose = () => {
    setShowReportModal(false);
    setResult(null);
  };

  if (!isAdmin && isRegisterFaultOpen) {
    return (
      <Modal isOpen={isRegisterFaultOpen} onClose={handleClose} title="Acceso Denegado" size="md">
        <div className="text-center py-6">
          <p className="text-[#6B7280] mb-4">
            Solo los administradores pueden registrar fallas.
          </p>
          <Button variant="secondary" onClick={handleClose}>
            Cerrar
          </Button>
        </div>
      </Modal>
    );
  }

  return (
    <>
      <Modal isOpen={isRegisterFaultOpen} onClose={handleClose} title="Registrar Falla" size="lg">
        {lineas.length === 0 && !lineasLoading && (
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mb-4">
            <p className="text-sm text-yellow-800">
              No hay líneas disponibles. Por favor, importa líneas desde el panel de administración antes de registrar fallas.
            </p>
          </div>
        )}
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* ✅ Si hay foco (lineId): mostramos solo lectura para evitar registrar en otra línea */}
          {focusedLineId ? (
            <div>
              <label className="block text-sm font-medium text-[#111827] mb-2">Línea</label>
              <div className="bg-[#F7FAF8] border border-[#E5E7EB] rounded-lg px-3 py-2 text-sm text-[#111827]">
                {focusedLinea
                  ? `${focusedLinea.numero}${focusedLinea.nombre ? ` - ${focusedLinea.nombre}` : ''}`
                  : lineasLoading
                  ? 'Cargando...'
                  : 'Línea no disponible'}
              </div>
            </div>
          ) : (
            <SearchableSelect
              label="Línea"
              value={formData.lineaId}
              onChange={(value) => setFormData({ ...formData, lineaId: value })}
              options={[
                { value: '', label: 'Seleccionar línea' },
                ...lineas.map((linea) => ({
                  value: linea.id,
                  label: `${linea.numero}${linea.nombre ? ` - ${linea.nombre}` : ''}`,
                })),
              ]}
              placeholder="Buscar línea..."
              required
            />
          )}

          <Input
            label="Kilómetro"
            icon={<MapPin className="w-4 h-4" />}
            placeholder="12.5"
            type="number"
            step="0.1"
            min="0"
            value={formData.km ?? ''}
            onChange={(e) => {
              const v = e.target.value;
              setFormData({ ...formData, km: v === '' ? null : Number(v) });
            }}
            required
          />

          <Input
            label="Tipo de falla"
            icon={<AlertCircle className="w-4 h-4" />}
            placeholder="Ej: Conductor caído, Aislador roto, Torre inclinada..."
            value={formData.tipo}
            onChange={(e) => setFormData({ ...formData, tipo: e.target.value })}
            required
          />

          <div className="grid grid-cols-2 gap-4">
            <Input
              label="Fecha"
              icon={<AlertCircle className="w-4 h-4" />}
              type="date"
              value={formData.fecha}
              onChange={(e) => setFormData({ ...formData, fecha: e.target.value })}
            />
            <Input
              label="Hora"
              type="time"
              value={formData.hora}
              onChange={(e) => setFormData({ ...formData, hora: e.target.value })}
            />
          </div>

          <Input
            label="Descripción (opcional)"
            icon={<FileText className="w-4 h-4" />}
            placeholder="Describe la falla..."
            value={formData.descripcion}
            onChange={(e) => setFormData({ ...formData, descripcion: e.target.value })}
          />

          {createFallaMutation.isError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3">
              <p className="text-sm text-red-600">
                {createFallaMutation.error instanceof Error
                  ? createFallaMutation.error.message
                  : 'Error al registrar la falla'}
              </p>
            </div>
          )}

          <div className="flex gap-3 pt-4">
            <Button type="submit" variant="primary" className="flex-1" disabled={createFallaMutation.isPending}>
              {createFallaMutation.isPending ? 'Registrando...' : 'Registrar falla'}
            </Button>
            <Button
              type="button"
              variant="secondary"
              onClick={handleClose}
              className="flex-1"
              disabled={createFallaMutation.isPending}
            >
              Cancelar
            </Button>
          </div>
        </form>
      </Modal>

      {result && <FaultReportModal isOpen={showReportModal} onClose={handleReportClose} faultData={result} />}
    </>
  );
}