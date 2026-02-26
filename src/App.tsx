// src/App.tsx
import { Suspense, lazy, useState } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import AppLayout from './components/layout/AppLayout';
import WelcomePage from './pages/WelcomePage';
import LoginPage from './pages/LoginPage';
import NotFoundPage from './pages/NotFoundPage';
import ProtectedRoute from './components/auth/ProtectedRoute';

const MapPage = lazy(() => import('./pages/MapPage'));
const ReportsPage = lazy(() => import('./pages/ReportsPage'));
const FallasPage = lazy(() => import('./pages/FallasPage'));
const AdminLinesPage = lazy(() => import('./pages/AdminLinesPage'));
const AdminImportPage = lazy(() => import('./pages/AdminImportPage'));
const AdminUsersPage = lazy(() => import('./pages/AdminUsersPage'));
const InfoLineasPage = lazy(() => import('./pages/InfoLineasPage'));

import RegisterFaultModal from './components/modals/RegisterFaultModal';
import ShareModal from './components/modals/ShareModal';
import { ToastProvider } from './contexts/ToastContext';
import { SearchProvider } from './contexts/SearchContext';
import { MapFocusProvider } from './contexts/MapFocusContext';
import { AuthProvider } from './contexts/AuthContext';

export default function App() {
  const [showShare, setShowShare] = useState(false);

  return (
    <BrowserRouter>
      <AuthProvider>
        <ToastProvider>
          <SearchProvider>
            {/*  MapFocusProvider DEBE estar dentro del Router */}
            <MapFocusProvider>
            <Suspense fallback={<div className="p-6 text-sm text-[#6B7280]">Cargando…</div>}>
            <Routes>
              <Route path="/" element={<WelcomePage />} />
              <Route path="/login" element={<LoginPage />} />

              <Route
                path="/dashboard"
                element={
                  <ProtectedRoute>
                    <AppLayout onShare={() => setShowShare(true)} />
                  </ProtectedRoute>
                }
              >
                <Route index element={<Navigate to="/dashboard/mapa" replace />} />
                <Route path="mapa" element={<MapPage />} />
                <Route path="reportes" element={<ReportsPage />} />
                <Route path="fallas" element={<FallasPage />} />
                <Route path="info-lineas" element={<InfoLineasPage />} />
                <Route
                  path="admin/lineas"
                  element={
                    <ProtectedRoute requireAdmin>
                      <AdminLinesPage />
                    </ProtectedRoute>
                  }
                />
                <Route
                  path="admin/importar"
                  element={
                    <ProtectedRoute requireAdmin>
                      <AdminImportPage />
                    </ProtectedRoute>
                  }
                />
                <Route
                  path="admin/usuarios"
                  element={
                    <ProtectedRoute requireAdmin>
                      <AdminUsersPage />
                    </ProtectedRoute>
                  }
                />
              </Route>

              <Route path="*" element={<NotFoundPage />} />
            </Routes>
            </Suspense>

            {/*  Modales dentro de providers (por si usan context) */}
            <RegisterFaultModal />

            <ShareModal isOpen={showShare} onClose={() => setShowShare(false)} />
          </MapFocusProvider>
        </SearchProvider>
      </ToastProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}