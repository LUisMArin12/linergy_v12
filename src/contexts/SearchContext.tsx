import { createContext, useContext, useEffect, useMemo, useState, ReactNode } from 'react';

interface SearchContextType {
  searchQuery: string;
  setSearchQuery: (query: string) => void;
}

const SearchContext = createContext<SearchContextType | undefined>(undefined);

function getInitialQueryFromUrl(): string {
  try {
    const url = new URL(window.location.href);
    return url.searchParams.get('q') ?? '';
  } catch {
    return '';
  }
}

function setUrlParam(key: string, value: string) {
  try {
    const url = new URL(window.location.href);

    if (!value) url.searchParams.delete(key);
    else url.searchParams.set(key, value);

    // Evita trailing "?" vacío
    const next = url.toString().endsWith('?') ? url.toString().slice(0, -1) : url.toString();

    // No llenar historial con cada tecla
    window.history.replaceState({}, '', next);
  } catch {
    // noop
  }
}

export function SearchProvider({ children }: { children: ReactNode }) {
  const [searchQuery, setSearchQueryState] = useState(() => getInitialQueryFromUrl());

  // ✅ Setter estable
  const setSearchQuery = (query: string) => {
    setSearchQueryState(query);
  };

  // ✅ Sincroniza a URL (merge automático porque usa URL actual)
  useEffect(() => {
    setUrlParam('q', searchQuery.trim());
  }, [searchQuery]);

  const value = useMemo(() => ({ searchQuery, setSearchQuery }), [searchQuery]);

  return <SearchContext.Provider value={value}>{children}</SearchContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components
export function useSearch() {
  const context = useContext(SearchContext);
  if (context === undefined) {
    throw new Error('useSearch must be used within a SearchProvider');
  }
  return context;
}