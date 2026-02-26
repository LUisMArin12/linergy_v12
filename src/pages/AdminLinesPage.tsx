import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Search, Edit, Trash2 } from 'lucide-react';
import Button from '../components/ui/Button';
import Badge from '../components/ui/Badge';
import Modal from '../components/ui/Modal';
import ConfirmDeleteModal from '../components/modals/ConfirmDeleteModal';
import Input from '../components/ui/Input';
import Chip from '../components/ui/Chip';
import { supabase, Linea } from '../lib/supabase';

type ClassificationFilter = 'ALL' | Linea['clasificacion'];

export default function AdminLinesPage() {
  const queryClient = useQueryClient();

  // Texto y filtros
  const [searchQuery, setSearchQuery] = useState('');
  const [classificationFilter, setClassificationFilter] = useState<ClassificationFilter>('ALL');

  // Modales
  const [editingLine, setEditingLine] = useState<Linea | null>(null);
  const [lineToDelete, setLineToDelete] = useState<Linea | null>(null);
  const [formData, setFormData] = useState<{
    numero: string;
    nombre: string;
    clasificacion: 'ALTA' | 'MODERADA' | 'BAJA';
    km_inicio: number | null;
    km_fin: number | null;
  }>({
    numero: '',
    nombre: '',
    clasificacion: 'MODERADA',
    km_inicio: null,
    km_fin: null,
  });

  const { data: lineas = [], isLoading } = useQuery({
    queryKey: ['lineas-admin'],
    queryFn: async () => {
      const { data, error } = await supabase.from('lineas').select('*').order('numero');
      if (error) throw error;
      return data as Linea[];
    },
  });

  const updateLineaMutation = useMutation({
    mutationFn: async (data: { id: string; updates: Partial<Linea> }) => {
      const { error } = await supabase.from('lineas').update(data.updates).eq('id', data.id);
      if (error) throw error;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['lineas-admin'] });
      queryClient.invalidateQueries({ queryKey: ['lineas'] });
      setEditingLine(null);
    },
  });

  const deleteLineaMutation = useMutation({
    mutationFn: async (id: string) => {
      const { error } = await supabase.from('lineas').delete().eq('id', id);
      if (error) throw error;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['lineas-admin'] });
      queryClient.invalidateQueries({ queryKey: ['lineas'] });
      setLineToDelete(null);
    },
  });

  const filteredLineas = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();

    return lineas.filter((linea) => {
      const numero = (linea.numero ?? '').toLowerCase();
      const nombre = (linea.nombre ?? '').toLowerCase();

      const matchesText = !q || numero.includes(q) || nombre.includes(q);
      const matchesClassification =
        classificationFilter === 'ALL' || linea.clasificacion === classificationFilter;

      return matchesText && matchesClassification;
    });
  }, [lineas, searchQuery, classificationFilter]);

  const isAnyFilterActive =
    searchQuery.trim().length > 0 || classificationFilter !== 'ALL';

  const clearFilters = () => {
    setSearchQuery('');
    setClassificationFilter('ALL');
  };

  const handleEdit = (linea: Linea) => {
    setEditingLine(linea);
    setFormData({
      numero: linea.numero,
      nombre: linea.nombre || '',
      clasificacion: linea.clasificacion,
      km_inicio: linea.km_inicio,
      km_fin: linea.km_fin,
    });
  };

  const handleSubmit = () => {
    if (!editingLine) return;

    updateLineaMutation.mutate({
      id: editingLine.id,
      updates: formData, // ya no manda prioridad
    });
  };

  const handleConfirmDelete = () => {
    if (lineToDelete) {
      deleteLineaMutation.mutate(lineToDelete.id);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#111827]">Gestión de Líneas</h1>
          <p className="text-sm text-[#6B7280] mt-1">
            Administra las líneas de subtransmisión del sistema
          </p>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-[#E5E7EB] p-4">
        {/* Barra de búsqueda + filtros */}
        <div className="space-y-3 mb-4">
          <div className="grid grid-cols-1 md:grid-cols-1 gap-3">
            <Input
              placeholder="Buscar por número o nombre..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              icon={<Search className="w-5 h-5" />}
            />
          </div>

          <div className="flex flex-col md:flex-row md:items-center gap-3">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium text-[#111827] mr-1">Clasificación:</span>
              <Chip selected={classificationFilter === 'ALL'} onClick={() => setClassificationFilter('ALL')}>
                Todas
              </Chip>
              <Chip selected={classificationFilter === 'BAJA'} onClick={() => setClassificationFilter('BAJA')}>
                Baja
              </Chip>
              <Chip
                selected={classificationFilter === 'MODERADA'}
                onClick={() => setClassificationFilter('MODERADA')}
              >
                Moderada
              </Chip>
              <Chip selected={classificationFilter === 'ALTA'} onClick={() => setClassificationFilter('ALTA')}>
                Alta
              </Chip>
            </div>

            <div className="flex items-center gap-3 md:ml-auto">
              <div className="text-sm text-[#6B7280]">
                Mostrando {filteredLineas.length} de {lineas.length}
              </div>
              {isAnyFilterActive && (
                <Button variant="ghost" size="sm" onClick={clearFilters}>
                  Limpiar filtros
                </Button>
              )}
            </div>
          </div>
        </div>

        <div>
          {isLoading ? (
            <div className="text-center py-8 text-[#6B7280]">Cargando líneas...</div>
          ) : (
            <>
              {/* Mobile-first: cards */}
              <div className="grid grid-cols-1 gap-3 md:hidden">
                {filteredLineas.map((linea) => (
                  <div
                    key={linea.id}
                    className="rounded-xl border border-[#E5E7EB] bg-white p-4 shadow-sm"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-base font-semibold text-[#111827]">{linea.numero}</p>
                        <p className="text-sm text-[#6B7280] mt-1">{linea.nombre || '-'}</p>
                      </div>
                      <Badge variant="classification" classification={linea.clasificacion}>
                        {linea.clasificacion}
                      </Badge>
                    </div>

                    <div className="mt-3 text-sm text-[#6B7280]">
                      <span className="font-medium text-[#111827]">Rango Km: </span>
                      {linea.km_inicio !== null && linea.km_fin !== null
                        ? `${linea.km_inicio.toFixed(1)} - ${linea.km_fin.toFixed(1)} km`
                        : '-'}
                    </div>

                    <div className="mt-4 flex gap-2">
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => handleEdit(linea)}
                        className="flex-1"
                      >
                        Editar
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setLineToDelete(linea)}
                        className="flex-1 text-red-600 hover:bg-red-50"
                      >
                        Eliminar
                      </Button>
                    </div>
                  </div>
                ))}

                {filteredLineas.length === 0 && (
                  <div className="py-10 text-center text-sm text-[#6B7280]">
                    No se encontraron líneas con los filtros actuales.
                  </div>
                )}
              </div>

              {/* Tablet/Desktop: table */}
              <div className="hidden md:block overflow-x-auto">
                <table className="w-full">
              <thead>
                <tr className="border-b border-[#E5E7EB]">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">
                    Línea
                  </th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">
                    Nombre
                  </th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">
                    Rango Km
                  </th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">
                    Clasificación
                  </th>
                  <th className="text-right py-3 px-4 text-sm font-semibold text-[#111827]">
                    Acciones
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredLineas.map((linea) => (
                  <tr
                    key={linea.id}
                    className="border-b border-[#E5E7EB] hover:bg-[#F7FAF8] transition-colors"
                  >
                    <td className="py-3 px-4">
                      <p className="font-semibold text-[#111827]">{linea.numero}</p>
                    </td>
                    <td className="py-3 px-4">
                      <p className="text-sm text-[#6B7280]">{linea.nombre || '-'}</p>
                    </td>
                    <td className="py-3 px-4">
                      <p className="text-sm text-[#6B7280]">
                        {linea.km_inicio !== null && linea.km_fin !== null
                          ? `${linea.km_inicio.toFixed(1)} - ${linea.km_fin.toFixed(1)} km`
                          : '-'}
                      </p>
                    </td>
                    <td className="py-3 px-4">
                      <Badge variant="classification" classification={linea.clasificacion}>
                        {linea.clasificacion}
                      </Badge>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex items-center justify-end gap-2">
                        <button
                          onClick={() => handleEdit(linea)}
                          className="p-2 hover:bg-[#DDF3EA] rounded-lg transition-colors text-[#6B7280] hover:text-[#157A5A]"
                        >
                          <Edit className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => setLineToDelete(linea)}
                          className="p-2 hover:bg-red-50 rounded-lg transition-colors text-[#6B7280] hover:text-red-600"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}

                {!isLoading && filteredLineas.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-10 text-center text-sm text-[#6B7280]">
                      No se encontraron líneas con los filtros actuales.
                    </td>
                  </tr>
                )}
              </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>

      <Modal
        isOpen={!!editingLine}
        onClose={() => setEditingLine(null)}
        title="Editar Línea"
        size="md"
      >
        {editingLine && (
          <div className="space-y-4">
            <Input
              label="Número de línea"
              type="text"
              value={formData.numero}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  numero: e.target.value,
                })
              }
              placeholder="Ej: ATR735000"
              required
            />

            <Input
              label="Nombre de la línea"
              type="text"
              value={formData.nombre}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  nombre: e.target.value,
                })
              }
              placeholder="Nombre descriptivo (opcional)"
            />

            <div className="grid grid-cols-2 gap-4">
              <Input
                label="KM Inicio"
                type="number"
                step="0.1"
                value={formData.km_inicio || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    km_inicio: e.target.value ? parseFloat(e.target.value) : null,
                  })
                }
                placeholder="0.0"
              />
              <Input
                label="KM Fin"
                type="number"
                step="0.1"
                value={formData.km_fin || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    km_fin: e.target.value ? parseFloat(e.target.value) : null,
                  })
                }
                placeholder="100.0"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-[#111827] mb-1">Clasificación</label>
              <select
                value={formData.clasificacion}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    clasificacion: e.target.value as 'ALTA' | 'MODERADA' | 'BAJA',
                  })
                }
                className="w-full px-3 py-2 text-sm rounded-lg border border-[#E5E7EB] focus:outline-none focus:ring-2 focus:ring-[#157A5A]/20 focus:border-[#157A5A]"
              >
                <option value="ALTA">Alta</option>
                <option value="MODERADA">Moderada</option>
                <option value="BAJA">Baja</option>
              </select>
            </div>

            {updateLineaMutation.isError && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                <p className="text-sm text-red-600">
                  {updateLineaMutation.error instanceof Error
                    ? updateLineaMutation.error.message
                    : 'Error al actualizar la línea'}
                </p>
              </div>
            )}

            <div className="flex gap-3 pt-4">
              <Button
                variant="primary"
                className="flex-1"
                onClick={handleSubmit}
                disabled={updateLineaMutation.isPending}
              >
                {updateLineaMutation.isPending ? 'Guardando...' : 'Guardar cambios'}
              </Button>
              <Button
                variant="secondary"
                onClick={() => setEditingLine(null)}
                className="flex-1"
                disabled={updateLineaMutation.isPending}
              >
                Cancelar
              </Button>
            </div>
          </div>
        )}
      </Modal>

      <ConfirmDeleteModal
        isOpen={!!lineToDelete}
        onClose={() => setLineToDelete(null)}
        onConfirm={handleConfirmDelete}
        title="Eliminar Línea"
        message="¿Estás seguro de que deseas eliminar esta línea? Esta acción no se puede deshacer."
        isDeleting={deleteLineaMutation.isPending}
      />
    </div>
  );
}
