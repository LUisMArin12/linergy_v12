import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { MapPin, FileText, AlertCircle } from 'lucide-react';
import Modal from '../ui/Modal';
import Input from '../ui/Input';
import SearchableSelect from '../ui/SearchableSelect';
import Select from '../ui/Select';
import Button from '../ui/Button';
import { Linea,  supabase, updateFalla, Falla } from '../../lib/supabase';
import { useToast } from '../../contexts/ToastContext';
import { useAuth } from '../../contexts/AuthContext';

interface EditFaultModalProps {
  isOpen: boolean;
  onClose: () => void;
  falla: Falla;
}

interface EditFormData {
  lineaId: string;
  km: number | null;
  tipo: string;
  fecha: string;
  hora: string;
  descripcion: string;
  estado: 'ABIERTA' | 'EN_ATENCION' | 'CERRADA';
}

export default function EditFaultModal({ isOpen, onClose, falla }: EditFaultModalProps) {
  const queryClient = useQueryClient();
  const { showToast } = useToast();
  const { isAdmin } = useAuth();

  const [formData, setFormData] = useState<EditFormData>({
    lineaId: falla.linea_id,
    km: falla.km,
    tipo: falla.tipo,
    fecha: new Date(falla.ocurrencia_ts).toISOString().split('T')[0],
    hora: new Date(falla.ocurrencia_ts).toTimeString().slice(0, 5),
    descripcion: falla.descripcion || '',
    estado: falla.estado,
  });

  useEffect(() => {
    setFormData({
      lineaId: falla.linea_id,
      km: falla.km,
      tipo: falla.tipo,
      fecha: new Date(falla.ocurrencia_ts).toISOString().split('T')[0],
      hora: new Date(falla.ocurrencia_ts).toTimeString().slice(0, 5),
      descripcion: falla.descripcion || '',
      estado: falla.estado,
    });
  }, [falla]);

  const { data: lineas = [] } = useQuery({
    queryKey: ['lineas_edit_modal'],
    queryFn: async () => {
      const { data, error } = await supabase.from('lineas').select('*').order('numero');
      if (error) throw error;
      return data ?? [];
    },
    enabled: isOpen,
  });

  const updateFallaMutation = useMutation({
    mutationFn: async (data: EditFormData) => {
      if (!data.lineaId) throw new Error('Selecciona una línea para continuar');

      const km = data.km === null ? null : Number(data.km);
      if (km === null || !Number.isFinite(km) || km < 0) {
        throw new Error('Ingresa un kilómetro válido (debe ser mayor o igual a 0)');
      }

      const tipo = data.tipo.trim();
      if (!tipo) throw new Error('Indica el tipo de falla');

      await updateFalla(falla.id, {
        lineaId: data.lineaId,
        km,
        tipo,
        descripcion: data.descripcion?.trim() ? data.descripcion.trim() : null,
        estado: data.estado,
        fecha: data.fecha,
        hora: data.hora,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['fallas'] });
      showToast('Falla actualizada correctamente', 'success');
      onClose();
    },
    onError: (err) => {
      const msg = err instanceof Error ? err.message : 'Error al actualizar la falla';
      showToast(msg, 'error');
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    const kmValid = formData.km !== null && Number.isFinite(Number(formData.km)) && Number(formData.km) >= 0;
    const tipoValid = formData.tipo.trim().length > 0;

    if (!formData.lineaId || !kmValid || !tipoValid) return;

    updateFallaMutation.mutate(formData);
  };

  const handleClose = () => {
    updateFallaMutation.reset();
    onClose();
  };

  if (!isAdmin) {
    return (
      <Modal isOpen={isOpen} onClose={handleClose} title="Acceso Denegado" size="md">
        <div className="text-center py-6">
          <p className="text-[#6B7280] mb-4">
            Solo los administradores pueden editar fallas.
          </p>
          <Button variant="secondary" onClick={handleClose}>
            Cerrar
          </Button>
        </div>
      </Modal>
    );
  }

  return (
    <Modal isOpen={isOpen} onClose={handleClose} title="Editar Falla" size="lg">
      <form onSubmit={handleSubmit} className="space-y-4">
        <SearchableSelect
          label="Línea"
          value={formData.lineaId}
          onChange={(value) => setFormData({ ...formData, lineaId: value })}
          options={[
            { value: '', label: 'Seleccionar línea' },
            ...lineas.map((linea: Linea) => ({
              value: linea.id,
              label: `${linea.numero}${linea.nombre ? ` - ${linea.nombre}` : ''}`,
            })),
          ]}
          placeholder="Buscar línea..."
          required
        />

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

        <Select
          label="Estado"
          value={formData.estado}
          onChange={(e) => setFormData({ ...formData, estado: e.target.value as 'ABIERTA' | 'EN_ATENCION' | 'CERRADA' })}
          options={[
            { value: 'ABIERTA', label: 'Abierta' },
            { value: 'EN_ATENCION', label: 'En atención' },
            { value: 'CERRADA', label: 'Cerrada' },
          ]}
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

        {updateFallaMutation.isError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-3">
            <p className="text-sm text-red-600">
              {updateFallaMutation.error instanceof Error
                ? updateFallaMutation.error.message
                : 'Error al actualizar la falla'}
            </p>
          </div>
        )}

        <div className="flex gap-3 pt-4">
          <Button type="submit" variant="primary" className="flex-1" disabled={updateFallaMutation.isPending}>
            {updateFallaMutation.isPending ? 'Guardando...' : 'Guardar cambios'}
          </Button>
          <Button type="button" variant="secondary" onClick={handleClose} className="flex-1" disabled={updateFallaMutation.isPending}>
            Cancelar
          </Button>
        </div>
      </form>
    </Modal>
  );
}