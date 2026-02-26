import { NavLink, useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { Map, FileText, Info, Cable, UploadCloud, LogOut, X, Users, AlertTriangle } from 'lucide-react';
import clsx from 'clsx';
import { supabase } from '../../lib/supabase';
import { useAuth } from '../../contexts/AuthContext';

interface SidebarProps {
  collapsed: boolean;
  mobileOpen: boolean;
  onToggleCollapsed: () => void;
  onCloseMobile: () => void;
}

const menuItems = [
  {
    section: 'Operación',
    items: [
      { to: '/dashboard/mapa', icon: Map, label: 'Mapa', adminOnly: false },
      { to: '/dashboard/reportes', icon: FileText, label: 'Reportes', adminOnly: false },
      { to: '/dashboard/fallas', icon: AlertTriangle, label: 'Fallas', adminOnly: false },
    ],
  },
  {
    section: 'Administración',
    items: [
      { to: '/dashboard/info-lineas', icon: Info, label: 'Información Líneas', adminOnly: false },
      { to: '/dashboard/admin/lineas', icon: Cable, label: 'Líneas', adminOnly: true },
      { to: '/dashboard/admin/importar', icon: UploadCloud, label: 'Importar KMZ', adminOnly: true },
      { to: '/dashboard/admin/usuarios', icon: Users, label: 'Usuarios', adminOnly: true },
    ],
  },
];

function SidebarContent({ collapsed, onNavigate }: { collapsed: boolean; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const { isAdmin } = useAuth();

  const handleLogout = async () => {
    try {
      await supabase.auth.signOut();
    } catch {
      // noop
    }
    navigate('/', { replace: true });
  };

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-[#E5E7EB] px-4 py-4">
        <div className="w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0 overflow-hidden bg-white">
          <img src="/image.png" alt="Linergy" className="w-full h-full object-contain" />
        </div>
        {!collapsed && (
          <div className="overflow-hidden">
            <h1 className="text-lg font-bold text-[#0B3D2E] leading-tight">Linergy</h1>
            <p className="text-xs text-[#6B7280]">Panel de control</p>
          </div>
        )}
      </div>

      <nav className="flex-1 overflow-y-auto py-4">
        {menuItems.map((section) => {
          const visibleItems = section.items.filter(item => !item.adminOnly || isAdmin);
          if (visibleItems.length === 0) return null;

          return (
            <div key={section.section} className="mb-6">
              {!collapsed && (
                <p className="px-4 mb-2 text-xs font-semibold text-[#6B7280] uppercase tracking-wider">
                  {section.section}
                </p>
              )}
              <div className="space-y-1 px-2">
                {visibleItems.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  onClick={onNavigate}
                  className={({ isActive }) =>
                    clsx(
                      'flex items-center gap-3 px-3 py-3 rounded-lg transition-all duration-200',
                      'hover:bg-[#F7FAF8] group',
                      {
                        'bg-[#DDF3EA] border-l-4 border-[#157A5A] text-[#0B3D2E] font-medium': isActive,
                        'text-[#6B7280]': !isActive,
                        'justify-center': collapsed,
                      }
                    )
                  }
                >
                  {({ isActive }) => (
                    <>
                      <item.icon
                        className={clsx(
                          'w-5 h-5 flex-shrink-0',
                          isActive ? 'text-[#157A5A]' : 'text-[#6B7280] group-hover:text-[#157A5A]'
                        )}
                      />
                      {!collapsed && <span className="text-sm whitespace-nowrap overflow-hidden">{item.label}</span>}
                    </>
                  )}
                </NavLink>
                ))}
              </div>
            </div>
          );
        })}
      </nav>

      <div className="border-t border-[#E5E7EB] p-4">
        <button
          type="button"
          onClick={handleLogout}
          className={clsx(
            'w-full flex items-center gap-3 px-3 py-3 rounded-lg transition-all duration-200',
            'hover:bg-red-50 group',
            collapsed && 'justify-center'
          )}
        >
          <LogOut className="w-5 h-5 text-red-600 group-hover:text-red-700 flex-shrink-0" />
          {!collapsed && (
            <span className="text-sm font-medium text-red-600 group-hover:text-red-700 whitespace-nowrap">
              Salir del sistema
            </span>
          )}
        </button>
      </div>
    </div>
  );
}

export default function Sidebar({ collapsed, mobileOpen, onToggleCollapsed, onCloseMobile }: SidebarProps) {
  const desktopWidth = collapsed ? 72 : 256;

  return (
    <>
      {/* Desktop sidebar (md+) */}
      <motion.aside
        initial={false}
        animate={{ width: desktopWidth }}
        className="hidden md:flex bg-white border-r border-[#E5E7EB] fixed left-0 top-0 z-30 h-screen"
      >
        <SidebarContent collapsed={collapsed} />
      </motion.aside>

      {/* Mobile drawer */}
      <AnimatePresence>
        {mobileOpen && (
          <>
            <motion.div
              className="fixed inset-0 z-40 bg-black/30 backdrop-blur-sm md:hidden"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={onCloseMobile}
            />
            <motion.aside
              className="fixed left-0 top-0 z-50 h-screen w-[min(86vw,320px)] bg-white border-r border-[#E5E7EB] md:hidden"
              initial={{ x: -360 }}
              animate={{ x: 0 }}
              exit={{ x: -360 }}
              transition={{ type: 'tween', duration: 0.2 }}
            >
              <div className="flex h-full flex-col">
                <div className="flex items-center justify-between px-4 py-3 border-b border-[#E5E7EB]">
                  <p className="text-sm font-semibold text-[#111827]">Menú</p>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={onToggleCollapsed}
                      className="hidden" // solo desktop
                      aria-label="Colapsar"
                    />
                    <button
                      type="button"
                      onClick={onCloseMobile}
                      className="p-2 rounded-lg hover:bg-[#F7FAF8]"
                      aria-label="Cerrar menú"
                    >
                      <X className="w-5 h-5 text-[#6B7280]" />
                    </button>
                  </div>
                </div>

                {/* ✅ Importante: el contenido debe ocupar el espacio restante y permitir scroll interno */}
                <div className="flex-1 overflow-hidden">
                  <SidebarContent collapsed={false} onNavigate={onCloseMobile} />
                </div>
              </div>
            </motion.aside>
          </>
        )}
      </AnimatePresence>
    </>
  );
}
