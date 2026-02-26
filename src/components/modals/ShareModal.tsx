// src/components/modals/ShareModal.tsx
import { useMemo, useState } from 'react';
import { Copy, Link as LinkIcon, Share2 } from 'lucide-react';
import Modal from '../ui/Modal';
import Button from '../ui/Button';
import Toast from '../ui/Toast';

interface ShareModalProps {
  isOpen: boolean;
  onClose: () => void;
  lineaId?: string | null;
  fallaId?: string | null;
}

export default function ShareModal({ isOpen, onClose, lineaId, fallaId }: ShareModalProps) {
  const [showToast, setShowToast] = useState(false);
  const [toastMessage, setToastMessage] = useState('Link copiado al portapapeles');

  const linkToShare = useMemo(() => {
    const url = new URL(window.location.href);

    url.searchParams.delete('lineaId');
    url.searchParams.delete('fallaId');

    if (!lineaId) url.searchParams.delete('lineId');
    else url.searchParams.set('lineId', lineaId);

    if (!fallaId) url.searchParams.delete('faultId');
    else url.searchParams.set('faultId', fallaId);

    const out = url.toString();
    return out.endsWith('?') ? out.slice(0, -1) : out;
  }, [lineaId, fallaId]);

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(linkToShare);
      setToastMessage('Link copiado al portapapeles');
      setShowToast(true);
    } catch {
      setToastMessage('Error al copiar el link');
      setShowToast(true);
    }
  };

  const shareViaWeb = async () => {
    if (navigator.share) {
      try {
        await navigator.share({
          title: 'Linergy - Sistema de Gestión de Líneas',
          text: 'Mira esta vista en Linergy',
          url: linkToShare,
        });
        setToastMessage('Compartido exitosamente');
        setShowToast(true);
      } catch (error) {
        const name =
          typeof error === 'object' && error && 'name' in error
            ? (error as { name?: unknown }).name
            : undefined;
        if (name !== 'AbortError') await copyLink();
      }
    } else {
      await copyLink();
    }
  };

  const canShare = typeof navigator !== 'undefined' && 'share' in navigator;

  return (
    <>
      <Modal isOpen={isOpen} onClose={onClose} title="Compartir Vista" size="md">
        <div className="space-y-6">
          <div>
            <p className="text-sm text-[#6B7280] mb-3">Comparte el estado actual del mapa con otros usuarios</p>

            <div className="bg-[#F7FAF8] rounded-lg p-4 border border-[#E5E7EB]">
              <div className="flex items-start gap-3">
                <LinkIcon className="w-5 h-5 text-[#157A5A] flex-shrink-0 mt-0.5" />
                <p className="text-sm text-[#111827] break-all font-mono">{linkToShare}</p>
              </div>
            </div>
          </div>

          <div className="flex gap-3 pt-4">
            {canShare && (
              <Button variant="primary" icon={<Share2 className="w-4 h-4" />} onClick={shareViaWeb} className="flex-1">
                Compartir
              </Button>
            )}

            <Button
              variant={canShare ? 'secondary' : 'primary'}
              icon={<Copy className="w-4 h-4" />}
              onClick={copyLink}
              className="flex-1"
            >
              Copiar link
            </Button>

            <Button variant="secondary" onClick={onClose} className="flex-1">
              Cerrar
            </Button>
          </div>
        </div>
      </Modal>

      <Toast message={toastMessage} type="success" isVisible={showToast} onClose={() => setShowToast(false)} />
    </>
  );
}