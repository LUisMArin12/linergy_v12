import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Home, MapPin, Search } from 'lucide-react';
import Button from '../components/ui/Button';

export default function NotFoundPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#F7FAF8] via-white to-[#DDF3EA] flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="text-center max-w-lg"
      >
        <motion.div
          initial={{ scale: 0 }}
          animate={{ scale: 1 }}
          transition={{ delay: 0.2, type: 'spring', stiffness: 200 }}
          className="relative mx-auto mb-8 w-48 h-48"
        >
          <div className="absolute inset-0 bg-gradient-to-br from-[#157A5A]/20 to-[#0B3D2E]/20 rounded-full blur-2xl" />
          <div className="relative w-full h-full bg-white rounded-full flex items-center justify-center shadow-lg border border-[#E5E7EB]">
            <Search className="w-20 h-20 text-[#157A5A]/30" strokeWidth={1.5} />
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.5 }}
              className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2"
            >
              <MapPin className="w-16 h-16 text-[#157A5A]" strokeWidth={2} />
            </motion.div>
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3 }}
        >
          <h1 className="text-8xl font-bold text-[#157A5A] mb-3">404</h1>
          <h2 className="text-2xl font-semibold text-[#111827] mb-3">Ruta no encontrada</h2>
          <p className="text-[#6B7280] mb-8 leading-relaxed">
            La página que buscas no existe o ha sido movida.
            <br />
            Verifica la URL o regresa al inicio.
          </p>

          <div className="flex gap-3 justify-center">
            <Button
              variant="primary"
              onClick={() => navigate('/')}
              icon={<Home className="w-4 h-4" />}
            >
              Inicio
            </Button>
            <Button
              variant="secondary"
              onClick={() => navigate('/dashboard/mapa')}
              icon={<MapPin className="w-4 h-4" />}
            >
              Ir al Mapa
            </Button>
          </div>
        </motion.div>
      </motion.div>
    </div>
  );
}
