import { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Shield } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import LoadingSpinner from '../ui/LoadingSpinner';

interface ProtectedRouteProps {
  children: ReactNode;
  requireAdmin?: boolean;
}

export default function ProtectedRoute({ children, requireAdmin = false }: ProtectedRouteProps) {
  const { user, profile, loading } = useAuth();

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-[#F7FAF8] via-white to-[#DDF3EA]">
        <LoadingSpinner size="lg" message="Verificando acceso..." />
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/" replace />;
  }

  if (requireAdmin && profile?.role !== 'admin') {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-[#F7FAF8] via-white to-[#DDF3EA] p-6">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="text-center max-w-md"
        >
          <div className="w-20 h-20 rounded-full bg-rose-100 mx-auto mb-4 flex items-center justify-center">
            <Shield className="w-10 h-10 text-rose-600" strokeWidth={2} />
          </div>
          <h2 className="text-xl font-bold text-[#111827] mb-2">Acceso Restringido</h2>
          <p className="text-[#6B7280] mb-6">
            Esta sección requiere permisos de administrador.
          </p>
          <Navigate to="/dashboard/mapa" replace />
        </motion.div>
      </div>
    );
  }

  return <>{children}</>;
}
