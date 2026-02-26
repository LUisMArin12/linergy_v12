import { motion } from 'framer-motion';
import { ArrowRight, Zap, Shield, MapPin } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import Button from '../components/ui/Button';

export default function WelcomePage() {
  const navigate = useNavigate();

  const features = [
    {
      icon: <MapPin className="w-6 h-6" />,
      title: 'Monitoreo',
      description: 'Visualiza todas las líneas de subtransmisión y estructuras',
    },
    {
      icon: <Zap className="w-6 h-6" />,
      title: 'Gestión de Fallas',
      description: 'Registra y da seguimiento a incidencias',
    },
    {
      icon: <Shield className="w-6 h-6" />,
      title: 'Control Total',
      description: 'Administra la infraestructura eléctrica',
    },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#F7FAF8] via-white to-[#DDF3EA] flex items-center justify-center p-4 overflow-hidden relative">
      <div className="absolute inset-0 overflow-hidden">
        <motion.div
          animate={{
            scale: [1, 1.2, 1],
            opacity: [0.3, 0.5, 0.3],
          }}
          transition={{
            duration: 8,
            repeat: Infinity,
            ease: 'easeInOut',
          }}
          className="absolute -top-1/4 -left-1/4 w-1/2 h-1/2 bg-[#157A5A] rounded-full blur-3xl"
        />
        <motion.div
          animate={{
            scale: [1.2, 1, 1.2],
            opacity: [0.2, 0.4, 0.2],
          }}
          transition={{
            duration: 10,
            repeat: Infinity,
            ease: 'easeInOut',
          }}
          className="absolute -bottom-1/4 -right-1/4 w-1/2 h-1/2 bg-[#0B3D2E] rounded-full blur-3xl"
        />
      </div>

      <div className="max-w-7xl w-full relative z-10">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, ease: 'easeOut' }}
            className="space-y-8"
          >
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2, duration: 0.6 }}
            >
              <motion.div
                animate={{ scale: [1, 1.05, 1] }}
                transition={{ duration: 2, repeat: Infinity }}
                className="inline-flex items-center gap-2 bg-white/80 backdrop-blur-sm px-4 py-2 rounded-full shadow-lg mb-6"
              >
                <Zap className="w-5 h-5 text-[#157A5A]" />
                <span className="text-sm font-semibold text-[#0B3D2E]">
                  Comisión Federal de Electricidad
                </span>
              </motion.div>

              <h1 className="text-5xl lg:text-6xl font-bold text-[#111827] mb-4 leading-tight">
                LINERGY
              </h1>
              <p className="text-2xl text-[#157A5A] font-semibold mb-4">
                Sistema de Gestión de Líneas de Subtransmisión
              </p>
              <p className="text-lg text-[#6B7280] leading-relaxed">
                Monitorea, gestiona y optimiza la infraestructura eléctrica con
                tecnología de vanguardia.
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4, duration: 0.6 }}
              className="grid grid-cols-1 sm:grid-cols-3 gap-4"
            >
              {features.map((feature, index) => (
                <motion.div
                  key={index}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.6 + index * 0.1, duration: 0.6 }}
                  whileHover={{ scale: 1.05 }}
                  className="bg-white/80 backdrop-blur-sm rounded-xl p-4 shadow-lg border border-[#E5E7EB]"
                >
                  <div className="w-12 h-12 bg-[#DDF3EA] rounded-lg flex items-center justify-center text-[#157A5A] mb-3">
                    {feature.icon}
                  </div>
                  <h3 className="font-semibold text-[#111827] mb-1 text-sm">
                    {feature.title}
                  </h3>
                  <p className="text-xs text-[#6B7280]">{feature.description}</p>
                </motion.div>
              ))}
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.8, duration: 0.6 }}
              className="flex flex-col sm:flex-row gap-4"
            >
              <Button
                variant="primary"
                icon={<ArrowRight className="w-5 h-5" />}
                onClick={() => navigate('/login')}
                className="text-lg px-8 py-4 shadow-xl hover:shadow-2xl transition-all"
              >
                Acceder al Sistema
              </Button>
            </motion.div>

          </motion.div>

          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, ease: 'easeOut', delay: 0.3 }}
            className="relative"
          >
            <motion.div
              animate={{
                y: [0, -20, 0],
              }}
              transition={{
                duration: 4,
                repeat: Infinity,
                ease: 'easeInOut',
              }}
              className="relative"
            >
              <div className="absolute inset-0 bg-gradient-to-tr from-[#157A5A]/20 to-[#0B3D2E]/20 rounded-3xl blur-3xl" />
              <img
                src="/inicio.png"
                alt="Sistema de Gestión"
                className="relative w-full h-auto rounded-3xl shadow-2xl border-4 border-white/50"
              />
            </motion.div>

            <motion.div
              animate={{
                scale: [1, 1.2, 1],
                rotate: [0, 180, 360],
              }}
              transition={{
                duration: 20,
                repeat: Infinity,
                ease: 'linear',
              }}
              className="absolute -top-8 -right-8 w-24 h-24 bg-gradient-to-br from-[#157A5A] to-[#0B3D2E] rounded-full opacity-20 blur-xl"
            />
            <motion.div
              animate={{
                scale: [1.2, 1, 1.2],
                rotate: [360, 180, 0],
              }}
              transition={{
                duration: 15,
                repeat: Infinity,
                ease: 'linear',
              }}
              className="absolute -bottom-8 -left-8 w-32 h-32 bg-gradient-to-br from-[#0B3D2E] to-[#157A5A] rounded-full opacity-20 blur-xl"
            />
          </motion.div>
        </div>
      </div>
    </div>
  );
}
