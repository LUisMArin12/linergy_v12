import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Users, Shield, User, RefreshCw, Trash2 } from 'lucide-react';
import { supabase } from '../lib/supabase';
import Card from '../components/ui/Card';
import Button from '../components/ui/Button';
import Select from '../components/ui/Select';
import Badge from '../components/ui/Badge';
import { useToast } from '../contexts/ToastContext';
import ConfirmDeleteModal from '../components/modals/ConfirmDeleteModal';

type UserWithProfile = {
  id: string;
  email: string;
  role: 'admin' | 'user';
  created_at: string;
  last_sign_in_at: string | null;
};

export default function AdminUsersPage() {
  const { showToast } = useToast();
  const queryClient = useQueryClient();
  const [roleChanges, setRoleChanges] = useState<Record<string, 'admin' | 'user'>>({});
  const [userToDelete, setUserToDelete] = useState<UserWithProfile | null>(null);

  const { data: users, isLoading, error, refetch } = useQuery({
    queryKey: ['admin-users'],
    queryFn: async () => {
      const { data, error } = await supabase.rpc('get_all_users_with_profiles');
      if (error) throw error;
      return data as UserWithProfile[];
    },
    retry: false,
  });

  const updateRoleMutation = useMutation({
    mutationFn: async ({ userId, newRole }: { userId: string; newRole: 'admin' | 'user' }) => {
      const { error } = await supabase.rpc('update_user_role', {
        user_id: userId,
        new_role: newRole,
      });
      if (error) throw error;
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['admin-users'] });
      setRoleChanges((prev) => {
        const next = { ...prev };
        delete next[variables.userId];
        return next;
      });
      showToast('Rol actualizado correctamente', 'success');
    },
    onError: (error: Error) => {
      showToast(error.message || 'Error al actualizar rol', 'error');
    },
  });

  const deleteUserMutation = useMutation({
    mutationFn: async (userId: string) => {
      const { error } = await supabase.rpc('delete_user', {
        user_id: userId,
      });
      if (error) throw error;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-users'] });
      showToast('Usuario eliminado correctamente', 'success');
      setUserToDelete(null);
    },
    onError: (error: Error) => {
      showToast(error.message || 'Error al eliminar usuario', 'error');
    },
  });

  const handleRoleChange = (userId: string, newRole: 'admin' | 'user') => {
    setRoleChanges((prev) => ({ ...prev, [userId]: newRole }));
  };

  const handleSaveRole = (userId: string) => {
    const newRole = roleChanges[userId];
    if (newRole) {
      updateRoleMutation.mutate({ userId, newRole });
    }
  };

  const hasChanges = (userId: string, currentRole: string) => {
    return roleChanges[userId] && roleChanges[userId] !== currentRole;
  };

  const handleConfirmDelete = () => {
    if (userToDelete) {
      deleteUserMutation.mutate(userToDelete.id);
    }
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return 'Nunca';
    return new Date(dateStr).toLocaleDateString('es-MX', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col items-center justify-center py-12 gap-4">
            <RefreshCw className="w-6 h-6 animate-spin text-blue-600" />
            <p className="text-sm text-gray-600">Cargando usuarios...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-6xl mx-auto">
          <Card className="p-6">
            <div className="text-center text-red-600">
              <p className="font-semibold mb-2">Error al cargar usuarios</p>
              <p className="text-sm">{error instanceof Error ? error.message : String(error)}</p>
              <Button onClick={() => refetch()} className="mt-4">
                Reintentar
              </Button>
            </div>
          </Card>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <Users className="w-8 h-8 text-blue-600" />
            <h1 className="text-3xl font-bold text-gray-900">Administración de Usuarios</h1>
          </div>
          <p className="text-gray-600">
            Gestiona los roles y permisos de los usuarios del sistema
          </p>
        </div>

        <Card className="overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Usuario
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Rol
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Fecha de Registro
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Último Acceso
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Acciones
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {users?.map((user) => (
                  <tr key={user.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center">
                          <User className="w-5 h-5 text-blue-600" />
                        </div>
                        <div>
                          <div className="text-sm font-medium text-gray-900">{user.email}</div>
                          <div className="text-xs text-gray-500">{user.id}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center gap-2">
                        <Select
                          value={roleChanges[user.id] || user.role}
                          onChange={(e) =>
                            handleRoleChange(user.id, e.target.value as 'admin' | 'user')
                          }
                          className="text-sm"
                        >
                          <option value="user">Usuario</option>
                          <option value="admin">Administrador</option>
                        </Select>
                        {user.role === 'admin' && !hasChanges(user.id, user.role) && (
                          <Shield className="w-4 h-4 text-amber-500" />
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatDate(user.created_at)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatDate(user.last_sign_in_at)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center gap-2">
                        {hasChanges(user.id, user.role) ? (
                          <Button
                            size="sm"
                            onClick={() => handleSaveRole(user.id)}
                            disabled={updateRoleMutation.isPending}
                          >
                            {updateRoleMutation.isPending ? (
                              <RefreshCw className="w-4 h-4 animate-spin" />
                            ) : (
                              'Guardar'
                            )}
                          </Button>
                        ) : (
                          <Badge variant={user.role === 'admin' ? 'warning' : 'info'}>
                            {user.role === 'admin' ? 'Admin' : 'Usuario'}
                          </Badge>
                        )}
                        <button
                          onClick={() => setUserToDelete(user)}
                          className="p-1.5 hover:bg-red-50 rounded-lg transition-colors"
                          title="Eliminar usuario"
                        >
                          <Trash2 className="w-4 h-4 text-red-600" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {users && users.length === 0 && (
            <div className="text-center py-12 text-gray-500">
              <Users className="w-12 h-12 mx-auto mb-3 opacity-50" />
              <p>No hay usuarios registrados</p>
            </div>
          )}
        </Card>

        <Card className="mt-6 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Información sobre Roles</h2>
          <div className="space-y-3 text-sm text-gray-600">
            <div className="flex items-start gap-3">
              <Shield className="w-5 h-5 text-amber-500 mt-0.5 flex-shrink-0" />
              <div>
                <span className="font-semibold text-gray-900">Administrador:</span> Acceso
                completo al sistema, puede gestionar usuarios, importar datos, editar y eliminar
                fallas.
              </div>
            </div>
            <div className="flex items-start gap-3">
              <User className="w-5 h-5 text-blue-500 mt-0.5 flex-shrink-0" />
              <div>
                <span className="font-semibold text-gray-900">Usuario:</span> Puede ver el mapa,
                reportar fallas y consultar información, pero no puede eliminar o editar datos
                existentes.
              </div>
            </div>
          </div>
        </Card>

        <ConfirmDeleteModal
          isOpen={!!userToDelete}
          onClose={() => setUserToDelete(null)}
          onConfirm={handleConfirmDelete}
          title="Eliminar Usuario"
          message={`¿Estás seguro de que deseas eliminar al usuario ${userToDelete?.email}? Esta acción no se puede deshacer y eliminará todos los datos asociados.`}
          isDeleting={deleteUserMutation.isPending}
        />
      </div>
    </div>
  );
}
