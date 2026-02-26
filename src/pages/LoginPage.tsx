import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { LogIn, UserPlus, Zap } from 'lucide-react';
import Button from '../components/ui/Button';
import Input from '../components/ui/Input';
import { useAuth } from '../contexts/AuthContext';
import { useToast } from '../contexts/ToastContext';

export default function LoginPage() {
  const navigate = useNavigate();
  const { signIn, signUp } = useAuth();
  const { showToast } = useToast();
  const [isLogin, setIsLogin] = useState(true);
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState({
    email: '',
    password: '',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      if (isLogin) {
        await signIn(formData.email, formData.password);
        navigate('/dashboard/mapa', { state: { fromLogin: true } });
      } else {
        await signUp(formData.email, formData.password);
        showToast('Cuenta creada exitosamente', 'success');
        navigate('/dashboard/mapa', { state: { fromLogin: true } });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error en la autenticación';
      showToast(message, 'error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#F7FAF8] via-white to-[#DDF3EA] flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="w-full max-w-md"
      >
        <div className="bg-white rounded-2xl shadow-xl border border-[#E5E7EB] overflow-hidden">
          <div className="bg-gradient-to-r from-[#157A5A] to-[#0B3D2E] p-6 text-white">
            <div className="flex items-center gap-3 mb-2">
              <div className="w-12 h-12 bg-white rounded-lg flex items-center justify-center">
                <Zap className="w-7 h-7 text-[#157A5A]" />
              </div>
              <div>
                <h1 className="text-2xl font-bold">LINERGY</h1>
                <p className="text-sm text-white/80">Sistema CFE</p>
              </div>
            </div>
          </div>

          <div className="p-6">
            <div className="mb-6 text-center">
              <h2 className="text-xl font-bold text-[#111827] mb-1">
                {isLogin ? 'Iniciar Sesión' : 'Crear Cuenta'}
              </h2>
              <p className="text-sm text-[#6B7280]">
                {isLogin
                  ? 'Ingresa tus credenciales para acceder'
                  : 'Regístrate para acceder al sistema'}
              </p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-4">
              <Input
                label="Correo electrónico"
                type="email"
                placeholder="tu@ejemplo.com"
                value={formData.email}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                required
              />

              <Input
                label="Contraseña"
                type="password"
                placeholder="••••••••"
                value={formData.password}
                onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                required
              />

              <Button
                type="submit"
                variant="primary"
                icon={isLogin ? <LogIn className="w-4 h-4" /> : <UserPlus className="w-4 h-4" />}
                className="w-full"
                disabled={loading}
              >
                {loading ? 'Cargando...' : isLogin ? 'Iniciar Sesión' : 'Crear Cuenta'}
              </Button>
            </form>

            <div className="mt-6 text-center">
              <button
                type="button"
                onClick={() => setIsLogin(!isLogin)}
                className="text-sm text-[#157A5A] hover:text-[#0B3D2E] font-medium transition-colors"
              >
                {isLogin ? '¿No tienes cuenta? Regístrate' : '¿Ya tienes cuenta? Inicia sesión'}
              </button>
            </div>

            <div className="mt-6 pt-6 border-t border-[#E5E7EB]">
              <button
                type="button"
                onClick={() => navigate('/')}
                className="w-full text-sm text-[#6B7280] hover:text-[#111827] transition-colors"
              >
                ← Volver al inicio
              </button>
            </div>
          </div>
        </div>

        <div className="mt-6 text-center">
          <div className="bg-white/80 backdrop-blur-sm rounded-lg p-4 shadow-sm border border-[#E5E7EB]">
            <p className="text-xs font-semibold text-[#111827] mb-2">
              Nota para Administradores
            </p>
            <p className="text-xs text-[#6B7280] leading-relaxed">
              Los nuevos usuarios se crean con rol de "Usuario". Para obtener permisos de
              administrador, contacta al administrador del sistema para que actualice tu perfil
              desde la base de datos de Supabase.
            </p>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
