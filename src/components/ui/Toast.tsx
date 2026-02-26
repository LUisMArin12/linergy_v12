import { useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { CheckCircle, AlertCircle, Info, X } from 'lucide-react';
import clsx from 'clsx';

export type ToastType = 'success' | 'error' | 'info';

interface ToastProps {
  message: string;
  type: ToastType;
  isVisible: boolean;
  onClose: () => void;
  duration?: number;
}

export default function Toast({ message, type, isVisible, onClose, duration = 3000 }: ToastProps) {
  useEffect(() => {
    if (isVisible && duration) {
      const timer = setTimeout(onClose, duration);
      return () => clearTimeout(timer);
    }
  }, [isVisible, duration, onClose]);

  const iconConfig = {
    success: {
      icon: <CheckCircle className="w-4 h-4" strokeWidth={2.5} />,
      bg: 'bg-emerald-500',
      text: 'text-white',
    },
    error: {
      icon: <AlertCircle className="w-4 h-4" strokeWidth={2.5} />,
      bg: 'bg-rose-500',
      text: 'text-white',
    },
    info: {
      icon: <Info className="w-4 h-4" strokeWidth={2.5} />,
      bg: 'bg-sky-500',
      text: 'text-white',
    },
  };

  const config = iconConfig[type];

  return (
    <AnimatePresence>
      {isVisible && (
        <motion.div
          initial={{ opacity: 0, y: -50, scale: 0.9 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, scale: 0.9 }}
          transition={{ type: 'spring', stiffness: 500, damping: 30 }}
          className="fixed top-6 right-6 z-50 max-w-sm"
        >
          <div
            className={clsx(
              'flex items-center gap-3 pl-3 pr-2 py-2.5 rounded-xl shadow-lg backdrop-blur-sm',
              config.bg,
              config.text
            )}
            style={{
              boxShadow: '0 10px 40px -10px rgba(0, 0, 0, 0.2), 0 0 0 1px rgba(255, 255, 255, 0.1)',
            }}
          >
            <div className="flex items-center gap-2.5 flex-1 min-w-0">
              <div className="flex-shrink-0">{config.icon}</div>
              <p className="text-sm font-medium leading-snug">{message}</p>
            </div>
            <button
              onClick={onClose}
              className="flex-shrink-0 p-1.5 hover:bg-white/20 rounded-lg transition-colors"
              aria-label="Cerrar notificación"
            >
              <X className="w-3.5 h-3.5" strokeWidth={2.5} />
            </button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
