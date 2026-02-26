import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Search, Filter, Edit2, Trash2, MapPin, AlertCircle, Calendar, FileText, X } from 'lucide-react';
import { supabase, Falla, deleteFalla } from '../lib/supabase';
import { useToast } from '../contexts/ToastContext';
import { useAuth } from '../contexts/AuthContext';
import Button from '../components/ui/Button';
import Select from '../components/ui/Select';
import Badge from '../components/ui/Badge';
import Card from '../components/ui/Card';
import EditFaultModal from '../components/modals/EditFaultModal';
import ConfirmDeleteModal from '../components/modals/ConfirmDeleteModal';
import { useNavigate } from 'react-router-dom';

type FilterEstado = 'TODAS' | 'ABIERTA' | 'EN_ATENCION' | 'CERRADA';

export default function FallasPage() {
  const { showToast } = useToast();
  const { isAdmin } = useAuth();
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const [searchTerm, setSearchTerm] = useState('');
  const [filterEstado, setFilterEstado] = useState<FilterEstado>('TODAS');
  const [selectedFalla, setSelectedFalla] = useState<Falla | null>(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);

  const { data: fallas = [], isLoading } = useQuery({
    queryKey: ['all-fallas'],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('fallas')
        .select(`
          *,
          lineas (
            numero,
            nombre
          )
        `)
        .is('deleted_at', null)
        .order('ocurrencia_ts', { ascending: false });

      if (error) throw error;
      return (data || []) as (Falla & { lineas: { numero: string; nombre: string | null } })[];
    },
  });

  const deleteFallaMutation = useMutation({
    mutationFn: async (fallaId: string) => {
      await deleteFalla(fallaId);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['all-fallas'] });
      queryClient.invalidateQueries({ queryKey: ['fallas'] });
      showToast('Falla eliminada correctamente', 'success');
      setIsDeleteModalOpen(false);
      setSelectedFalla(null);
    },
    onError: (error) => {
      const msg = error instanceof Error ? error.message : 'Error al eliminar la falla';
      showToast(msg, 'error');
    },
  });

  const filteredFallas = fallas.filter((falla) => {
    const matchesSearch =
      searchTerm === '' ||
      falla.tipo.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (falla.descripcion?.toLowerCase() || '').includes(searchTerm.toLowerCase()) ||
      falla.lineas.numero.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (falla.lineas.nombre?.toLowerCase() || '').includes(searchTerm.toLowerCase());

    const matchesEstado = filterEstado === 'TODAS' || falla.estado === filterEstado;

    return matchesSearch && matchesEstado;
  });

  const handleEdit = (falla: Falla) => {
    setSelectedFalla(falla);
    setIsEditModalOpen(true);
  };

  const handleDelete = (falla: Falla) => {
    setSelectedFalla(falla);
    setIsDeleteModalOpen(true);
  };

  const handleConfirmDelete = () => {
    if (selectedFalla) {
      deleteFallaMutation.mutate(selectedFalla.id);
    }
  };

  const handleViewOnMap = (falla: Falla & { lineas: { numero: string; nombre: string | null } }) => {
    navigate(`/dashboard/mapa?faultId=${falla.id}`);
  };

  const getEstadoBadgeVariant = (estado: string) => {
    switch (estado) {
      case 'ABIERTA':
        return 'error';
      case 'EN_ATENCION':
        return 'warning';
      case 'CERRADA':
        return 'success';
      default:
        return 'default';
    }
  };

  const getEstadoLabel = (estado: string) => {
    switch (estado) {
      case 'ABIERTA':
        return 'Abierta';
      case 'EN_ATENCION':
        return 'En Atención';
      case 'CERRADA':
        return 'Cerrada';
      default:
        return estado;
    }
  };

  return (
    <div className="min-h-screen bg-[#F9FAFB]">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-[#111827] mb-2">Catálogo de Fallas</h1>
          <p className="text-[#6B7280]">
            Administra y consulta todas las fallas registradas en el sistema
          </p>
        </div>

        <Card className="mb-6">
          <div className="p-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-[#9CA3AF] w-5 h-5" />
                <input
                  type="text"
                  placeholder="Buscar por tipo, descripción o línea..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full pl-10 pr-10 py-2 border border-[#D1D5DB] rounded-lg focus:ring-2 focus:ring-[#3B82F6] focus:border-transparent outline-none"
                />
                {searchTerm && (
                  <button
                    onClick={() => setSearchTerm('')}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-[#9CA3AF] hover:text-[#6B7280]"
                  >
                    <X className="w-5 h-5" />
                  </button>
                )}
              </div>

              <div className="flex items-center gap-2">
                <Filter className="text-[#9CA3AF] w-5 h-5 flex-shrink-0" />
                <Select
                  value={filterEstado}
                  onChange={(e) => setFilterEstado(e.target.value as FilterEstado)}
                  options={[
                    { value: 'TODAS', label: 'Todas las fallas' },
                    { value: 'ABIERTA', label: 'Abiertas' },
                    { value: 'EN_ATENCION', label: 'En Atención' },
                    { value: 'CERRADA', label: 'Cerradas' },
                  ]}
                  className="flex-1"
                />
              </div>
            </div>

            <div className="mt-4 flex items-center justify-between text-sm">
              <p className="text-[#6B7280]">
                Mostrando <span className="font-semibold text-[#111827]">{filteredFallas.length}</span> de{' '}
                <span className="font-semibold text-[#111827]">{fallas.length}</span> fallas
              </p>
            </div>
          </div>
        </Card>

        {isLoading ? (
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#3B82F6]"></div>
          </div>
        ) : filteredFallas.length === 0 ? (
          <Card>
            <div className="p-12 text-center">
              <AlertCircle className="w-12 h-12 text-[#9CA3AF] mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-[#111827] mb-2">No se encontraron fallas</h3>
              <p className="text-[#6B7280]">
                {searchTerm || filterEstado !== 'TODAS'
                  ? 'Intenta ajustar los filtros de búsqueda'
                  : 'No hay fallas registradas en el sistema'}
              </p>
            </div>
          </Card>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
            {filteredFallas.map((falla) => (
              <Card key={falla.id} className="hover:shadow-lg transition-shadow">
                <div className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex-1">
                      <h3 className="text-lg font-semibold text-[#111827] mb-1">{falla.tipo}</h3>
                      <p className="text-sm text-[#6B7280]">
                        Línea {falla.lineas.numero}
                        {falla.lineas.nombre && ` - ${falla.lineas.nombre}`}
                      </p>
                    </div>
                    <Badge variant={getEstadoBadgeVariant(falla.estado)}>
                      {getEstadoLabel(falla.estado)}
                    </Badge>
                  </div>

                  <div className="space-y-2 mb-4">
                    <div className="flex items-center text-sm text-[#6B7280]">
                      <MapPin className="w-4 h-4 mr-2 flex-shrink-0" />
                      <span>Kilómetro {falla.km.toFixed(2)}</span>
                    </div>

                    <div className="flex items-center text-sm text-[#6B7280]">
                      <Calendar className="w-4 h-4 mr-2 flex-shrink-0" />
                      <span>{new Date(falla.ocurrencia_ts).toLocaleString('es-MX')}</span>
                    </div>

                    {falla.descripcion && (
                      <div className="flex items-start text-sm text-[#6B7280]">
                        <FileText className="w-4 h-4 mr-2 flex-shrink-0 mt-0.5" />
                        <span className="line-clamp-2">{falla.descripcion}</span>
                      </div>
                    )}
                  </div>

                  <div className="flex gap-2 pt-4 border-t border-[#E5E7EB]">
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => handleViewOnMap(falla)}
                      className="flex-1"
                    >
                      <MapPin className="w-4 h-4 mr-1" />
                      Ver en mapa
                    </Button>

                    {isAdmin && (
                      <>
                        <Button
                          variant="secondary"
                          size="sm"
                          onClick={() => handleEdit(falla)}
                          className="px-3"
                        >
                          <Edit2 className="w-4 h-4" />
                        </Button>

                        <Button
                          variant="danger"
                          size="sm"
                          onClick={() => handleDelete(falla)}
                          className="px-3"
                        >
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              </Card>
            ))}
          </div>
        )}
      </div>

      {selectedFalla && (
        <>
          <EditFaultModal
            isOpen={isEditModalOpen}
            onClose={() => {
              setIsEditModalOpen(false);
              setSelectedFalla(null);
            }}
            falla={selectedFalla}
          />

          <ConfirmDeleteModal
            isOpen={isDeleteModalOpen}
            onClose={() => {
              setIsDeleteModalOpen(false);
              setSelectedFalla(null);
            }}
            onConfirm={handleConfirmDelete}
            title="Eliminar Falla"
            message={`¿Estás seguro de que deseas eliminar la falla "${selectedFalla.tipo}" en el kilómetro ${selectedFalla.km.toFixed(2)}?`}
            isDeleting={deleteFallaMutation.isPending}
          />
        </>
      )}
    </div>
  );
}
