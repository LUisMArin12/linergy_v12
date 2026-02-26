import { useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import Header from './Header';
import { useSearch } from '../../contexts/SearchContext';

interface AppLayoutProps {
  onShare: () => void;
}

export default function AppLayout({ onShare }: AppLayoutProps) {
  // Desktop: colapsado (icon-only) vs expandido
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  // Mobile: drawer overlay
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const { searchQuery, setSearchQuery } = useSearch();

  // Cierra el drawer móvil al pasar a desktop
  useEffect(() => {
    const onResize = () => {
      if (window.innerWidth >= 768) setMobileSidebarOpen(false);
    };
    window.addEventListener('resize', onResize);
    onResize();
    return () => window.removeEventListener('resize', onResize);
  }, []);

  return (
    <div className="min-h-screen bg-[#F7FAF8]">
      <Sidebar
        collapsed={sidebarCollapsed}
        mobileOpen={mobileSidebarOpen}
        onToggleCollapsed={() => setSidebarCollapsed((v) => !v)}
        onCloseMobile={() => setMobileSidebarOpen(false)}
      />

      {/* Layout fluido (mobile-first) */}
      <div
        className={
          // En desktop dejamos espacio para el sidebar; en mobile ocupa todo
          sidebarCollapsed ? 'md:pl-[72px]' : 'md:pl-[256px]'
        }
      >
          <Header
            onOpenMobileSidebar={() => setMobileSidebarOpen(true)}
            onToggleDesktopSidebar={() => setSidebarCollapsed((v) => !v)}
            onShare={onShare}
            searchQuery={searchQuery}
            onSearchChange={setSearchQuery}
          />

          <main className="px-4 py-4 sm:px-6 sm:py-6">
            <div className="mx-auto w-full max-w-screen-2xl">
              <Outlet />
            </div>
          </main>
      </div>
    </div>
  );
}
