import { AlertTriangle } from 'lucide-react';
import Modal from '../ui/Modal';
import Button from '../ui/Button';
import { useAuth } from '../../contexts/AuthContext';

interface ConfirmDeleteModalProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: string;
  message: string;
  isDeleting?: boolean;
}

export default function ConfirmDeleteModal({
  isOpen,
  onClose,
  onConfirm,
  title,
  message,
  isDeleting = false,
}: ConfirmDeleteModalProps) {
  const { isAdmin } = useAuth();

  if (!isAdmin) {
    return (
      <Modal isOpen={isOpen} onClose={onClose} title="" size="sm">
        <div className="text-center py-4">
          <div className="w-12 h-12 rounded-full bg-amber-100 mx-auto mb-3 flex items-center justify-center">
            <AlertTriangle className="w-5 h-5 text-amber-600" strokeWidth={2} />
          </div>
          <h3 className="text-base font-semibold text-[#111827] mb-1.5">Acceso Denegado</h3>
          <p className="text-sm text-[#6B7280] mb-5">
            Solo los administradores pueden eliminar elementos.
          </p>
          <Button variant="secondary" onClick={onClose} className="w-full">
            Entendido
          </Button>
        </div>
      </Modal>
    );
  }

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="" size="sm">
      <div className="text-center py-4">
        <div className="w-12 h-12 rounded-full bg-rose-100 mx-auto mb-3 flex items-center justify-center">
          <AlertTriangle className="w-5 h-5 text-rose-600" strokeWidth={2} />
        </div>
        <h3 className="text-base font-semibold text-[#111827] mb-1.5">{title}</h3>
        <p className="text-sm text-[#6B7280] mb-5">{message}</p>

        <div className="bg-rose-50 border border-rose-200 rounded-lg px-3 py-2 mb-5">
          <p className="text-xs text-rose-700 font-medium">
            Esta acción es permanente
          </p>
        </div>

        <div className="flex gap-2.5">
          <Button
            variant="secondary"
            onClick={onClose}
            className="flex-1"
            disabled={isDeleting}
          >
            Cancelar
          </Button>
          <Button
            variant="danger"
            onClick={onConfirm}
            className="flex-1"
            disabled={isDeleting}
          >
            {isDeleting ? 'Eliminando...' : 'Eliminar'}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
