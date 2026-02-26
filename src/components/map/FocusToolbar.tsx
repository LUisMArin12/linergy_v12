import { Eye, PlusCircle, X, Share2 } from 'lucide-react';
import { useState } from 'react';
import Button from '../ui/Button';
import { useMapFocus } from '../../contexts/MapFocusContext';
import { Linea } from '../../lib/supabase';
import ShareModal from '../modals/ShareModal';

interface FocusToolbarProps {
  linea?: Linea;
  onExitFocus: () => void;
}

export default function FocusToolbar({ linea, onExitFocus }: FocusToolbarProps) {
  const { setIsRegisterFaultOpen } = useMapFocus();
  const [isShareOpen, setIsShareOpen] = useState(false);

  const lineaId = linea?.id ?? null;

  return (
    <>
      <div className="absolute top-4 left-4 right-4 z-[1000] bg-white rounded-lg shadow-lg border border-[#E5E7EB] p-3 lg:p-4">
        <div className="flex items-center gap-3 lg:gap-4">
          <div className="flex items-center gap-2 flex-1 min-w-0">
            <div className="w-8 h-8 bg-[#FFE5FF] rounded-lg flex items-center justify-center flex-shrink-0">
              <Eye className="w-4 h-4 text-[#FF00FF]" />
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-xs text-[#6B7280]">Enfocando línea</p>
              <p className="font-semibold text-[#111827] truncate">
                {linea ? `${linea.numero}${linea.nombre ? ` - ${linea.nombre}` : ''}` : 'Cargando...'}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-shrink-0">
            <Button
              variant="secondary"
              icon={<Share2 className="w-4 h-4" />}
              onClick={() => setIsShareOpen(true)}
              disabled={!lineaId}
              className="hidden sm:flex"
            >
              Compartir
            </Button>

            <Button
              variant="primary"
              icon={<PlusCircle className="w-4 h-4" />}
              onClick={() => setIsRegisterFaultOpen(true)}
              className="hidden sm:flex"
              disabled={!lineaId}
            >
              Registrar falla
            </Button>

            <Button variant="secondary" icon={<X className="w-4 h-4" />} onClick={onExitFocus}>
              Salir de enfoque
            </Button>
          </div>
        </div>
      </div>

      <ShareModal isOpen={isShareOpen} onClose={() => setIsShareOpen(false)} lineaId={lineaId} />
    </>
  );
}