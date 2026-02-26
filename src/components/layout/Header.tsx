import { useEffect, useState } from 'react';
import { Search, PlusCircle, Share2, Menu, PanelLeft } from 'lucide-react';
import Button from '../ui/Button';
import { useMapFocus } from '../../contexts/MapFocusContext';

interface HeaderProps {
  onOpenMobileSidebar: () => void;
  onToggleDesktopSidebar: () => void;
  onShare: () => void;
  searchQuery: string;
  onSearchChange: (query: string) => void;
}

export default function Header({
  onOpenMobileSidebar,
  onToggleDesktopSidebar,
  onShare,
  searchQuery,
  onSearchChange,
}: HeaderProps) {
  const { setIsRegisterFaultOpen } = useMapFocus();

  // ✅ Draft local: permite "ejecutar búsqueda" y luego limpiar el input sin perder el filtro aplicado
  const [draft, setDraft] = useState(searchQuery);

  // Si el searchQuery cambia externamente, sincroniza solo si el usuario no está tecleando
  useEffect(() => {
    // Si el input está vacío, reflejamos el estado externo (ej. navegación)
    if (draft === '') setDraft(searchQuery);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchQuery]);

  const submitSearch = () => {
    const q = draft.trim();
    onSearchChange(q);
    setDraft(''); // ✅ limpiar input al ejecutar
  };

  return (
    <header className="sticky top-0 z-20 bg-white/90 backdrop-blur border-b border-[#E5E7EB]">
      <div className="mx-auto w-full max-w-screen-2xl px-4 py-3 sm:px-6 sm:py-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
          {/* Mobile: abre drawer */}
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onOpenMobileSidebar}
              className="p-2 rounded-lg hover:bg-[#F7FAF8] transition-colors md:hidden"
              aria-label="Abrir menú"
            >
              <Menu className="w-5 h-5 text-[#6B7280]" />
            </button>

            {/* Desktop: colapsar sidebar */}
            <button
              type="button"
              onClick={onToggleDesktopSidebar}
              className="hidden md:inline-flex p-2 rounded-lg hover:bg-[#F7FAF8] transition-colors"
              aria-label="Colapsar menú"
            >
              <PanelLeft className="w-5 h-5 text-[#6B7280]" />
            </button>

            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-[#6B7280]" />

                <input
                  type="text"
                  placeholder="Buscar línea o falla..."
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      submitSearch();
                    }
                  }}
                  className="w-full min-h-[44px] pl-10 pr-12 rounded-lg border border-[#E5E7EB] focus:outline-none focus:ring-2 focus:ring-[#157A5A] focus:border-transparent transition-all"
                  aria-label="Buscar"
                />

                <button
                  type="button"
                  onClick={submitSearch}
                  className="absolute right-2 top-1/2 -translate-y-1/2 p-2 rounded-lg hover:bg-[#F7FAF8] transition-colors"
                  aria-label="Ejecutar búsqueda"
                >
                  <Search className="w-5 h-5 text-[#157A5A]" />
                </button>
              </div>

              {/* Indicador de filtro activo (cuando el input está limpio) */}
              {draft === '' && searchQuery.trim() !== '' && (
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-xs text-[#6B7280]">Filtro activo:</span>
                  <span className="text-xs font-semibold text-[#0B3D2E] truncate max-w-[220px]">
                    {searchQuery}
                  </span>
                  <button
                    type="button"
                    onClick={() => onSearchChange('')}
                    className="text-xs text-[#157A5A] hover:underline"
                  >
                    Limpiar
                  </button>
                </div>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2 sm:justify-end">
            <Button
              variant="primary"
              icon={<PlusCircle className="w-4 h-4" />}
              onClick={() => setIsRegisterFaultOpen(true)}
              className="flex-1 sm:flex-none"
            >
              Registrar
            </Button>
            <Button
              variant="secondary"
              icon={<Share2 className="w-4 h-4" />}
              onClick={onShare}
              className="hidden sm:inline-flex"
            >
              Compartir
            </Button>
          </div>
        </div>
      </div>
    </header>
  );
}
