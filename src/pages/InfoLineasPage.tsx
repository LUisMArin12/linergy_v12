import { useEffect, useMemo, useState } from 'react';
import { Search, Pencil, X } from 'lucide-react';
import Input from '../components/ui/Input';
import Badge from '../components/ui/Badge';
import Card from '../components/ui/Card';
import Button from '../components/ui/Button';
import Modal from '../components/ui/Modal';
import { useSearch } from '../contexts/SearchContext';
import { LINEAS_CATALOG, type LineaCatalogEntry } from '../lib/lineaCatalog';

type OverridesMap = Record<string, Partial<LineaCatalogEntry>>;

const LS_KEY = 'lineas_catalog_overrides_v1';

function loadOverrides(): OverridesMap {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return {};
    const parsed: unknown = JSON.parse(raw);
    if (parsed && typeof parsed === 'object') return parsed as OverridesMap;
    return {};
  } catch {
    return {};
  }
}

function saveOverrides(next: OverridesMap) {
  localStorage.setItem(LS_KEY, JSON.stringify(next));
}

function mergeEntry(base: LineaCatalogEntry, overrides: OverridesMap): LineaCatalogEntry {
  const o = overrides[base.claveEnlace];
  return o ? ({ ...base, ...o } as LineaCatalogEntry) : base;
}

function matches(entry: LineaCatalogEntry, q: string) {
  const hay = [
    entry.claveEnlace,
    entry.numero,
    entry.descripcion,
    entry.area,
    entry.tension,
    String(entry.kms ?? ''),
    String(entry.nc ?? ''),
    entry.conductor,
    entry.tipoEstructura,
    String(entry.numEstructuras ?? ''),
    String(entry.anio ?? ''),
    entry.comp,
    entry.cveSap,
    String(entry.brecha ?? ''),
    entry.confCond,
    entry.pob,
    entry.ent,
  ]
    .join(' ')
    .toLowerCase();

  return hay.includes(q);
}

type EditFormState = Omit<LineaCatalogEntry, 'kms' | 'nc' | 'numEstructuras' | 'anio' | 'brecha'> & {
  kms: string;
  nc: string;
  numEstructuras: string;
  anio: string;
  brecha: string;
};

function toEditState(e: LineaCatalogEntry): EditFormState {
  return {
    ...e,
    kms: e.kms === null || e.kms === undefined ? '' : String(e.kms),
    nc: e.nc === null || e.nc === undefined ? '' : String(e.nc),
    numEstructuras: e.numEstructuras === null || e.numEstructuras === undefined ? '' : String(e.numEstructuras),
    anio: e.anio === null || e.anio === undefined ? '' : String(e.anio),
    brecha: e.brecha === null || e.brecha === undefined ? '' : String(e.brecha),
  };
}

function toNumberOrNull(v: string): number | null {
  const t = v.trim();
  if (!t) return null;
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

export default function InfoLineasPage() {
  const { searchQuery, setSearchQuery } = useSearch();

  const [overrides, setOverrides] = useState<OverridesMap>({});
  const [editing, setEditing] = useState<LineaCatalogEntry | null>(null);
  const [form, setForm] = useState<EditFormState | null>(null);

  useEffect(() => {
    setOverrides(loadOverrides());
  }, []);

  const mergedCatalog = useMemo(() => {
    return LINEAS_CATALOG.map((e) => mergeEntry(e, overrides));
  }, [overrides]);

  const filtered = useMemo(() => {
    const q = (searchQuery ?? '').trim().toLowerCase();
    if (!q) return mergedCatalog;
    return mergedCatalog.filter((e) => matches(e, q));
  }, [searchQuery, mergedCatalog]);

  const openEdit = (e: LineaCatalogEntry) => {
    setEditing(e);
    setForm(toEditState(e));
  };

  const closeEdit = () => {
    setEditing(null);
    setForm(null);
  };

  const handleSave = () => {
    if (!editing || !form) return;

    const next: OverridesMap = { ...overrides };

    // Guardamos override por claveEnlace
    next[editing.claveEnlace] = {
      claveEnlace: form.claveEnlace,
      descripcion: form.descripcion,
      area: form.area,
      tension: form.tension,
      kms: toNumberOrNull(form.kms),
      nc: toNumberOrNull(form.nc),
      conductor: form.conductor,
      tipoEstructura: form.tipoEstructura,
      numEstructuras: toNumberOrNull(form.numEstructuras),
      anio: toNumberOrNull(form.anio),
      comp: form.comp,
      cveSap: form.cveSap,
      brecha: toNumberOrNull(form.brecha),
      confCond: form.confCond,
      pob: form.pob,
      ent: form.ent,
    };

    // Nota: 'numero' no se edita (derivado de clave), pero si cambias clave manualmente
    // no se recalcula; mantenemos el número original.
    saveOverrides(next);
    setOverrides(next);
    closeEdit();
  };

  const handleResetRow = () => {
    if (!editing) return;
    const next: OverridesMap = { ...overrides };
    delete next[editing.claveEnlace];
    saveOverrides(next);
    setOverrides(next);
    closeEdit();
  };


  return (
    <div className="space-y-6">
    <Modal isOpen={!!editing} onClose={closeEdit} title="Editar información de línea" size="lg">
      {!form ? null : (
        <div className="space-y-4">
          <div className="rounded-xl border border-[#E5E7EB] bg-white p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-[#111827]">{editing?.claveEnlace}</p>
                <p className="text-xs text-[#6B7280] mt-1">Cambios se guardan localmente (este equipo/navegador).</p>
              </div>
              <Badge>{form.tension}</Badge>
            </div>
          </div>

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Clave enlace" value={form.claveEnlace} onChange={(e) => setForm({ ...form, claveEnlace: e.target.value })} />
            <Input label="Descripción" value={form.descripcion} onChange={(e) => setForm({ ...form, descripcion: e.target.value })} />

            <Input label="Área" value={form.area} onChange={(e) => setForm({ ...form, area: e.target.value })} />
            <Input label="Tensión" value={form.tension} onChange={(e) => setForm({ ...form, tension: e.target.value })} />

            <Input label="Kms" type="number" step="0.01" value={form.kms} onChange={(e) => setForm({ ...form, kms: e.target.value })} />
            <Input label="NC" type="number" step="1" value={form.nc} onChange={(e) => setForm({ ...form, nc: e.target.value })} />

            <Input label="Conductor" value={form.conductor} onChange={(e) => setForm({ ...form, conductor: e.target.value })} />
            <Input label="Tip. Estruc" value={form.tipoEstructura} onChange={(e) => setForm({ ...form, tipoEstructura: e.target.value })} />

            <Input label="# Est" type="number" step="1" value={form.numEstructuras} onChange={(e) => setForm({ ...form, numEstructuras: e.target.value })} />
            <Input label="Año" type="number" step="1" value={form.anio} onChange={(e) => setForm({ ...form, anio: e.target.value })} />

            <Input label="Comp" value={form.comp} onChange={(e) => setForm({ ...form, comp: e.target.value })} />
            <Input label="Cve SAP" value={form.cveSap} onChange={(e) => setForm({ ...form, cveSap: e.target.value })} />

            <Input label="Brecha" type="number" step="0.01" value={form.brecha} onChange={(e) => setForm({ ...form, brecha: e.target.value })} />
            <Input label="Conf cond" value={form.confCond} onChange={(e) => setForm({ ...form, confCond: e.target.value })} />

            <Input label="POB" value={form.pob} onChange={(e) => setForm({ ...form, pob: e.target.value })} />
            <Input label="ENT" value={form.ent} onChange={(e) => setForm({ ...form, ent: e.target.value })} />
          </div>

          <div className="flex items-center justify-between gap-3 pt-2">
            <Button variant="secondary" onClick={handleResetRow} icon={<X className="w-4 h-4" />}>
              Restablecer registro
            </Button>
            <div className="flex gap-2">
              <Button variant="secondary" onClick={closeEdit}>Cancelar</Button>
              <Button onClick={handleSave}>Guardar cambios</Button>
            </div>
          </div>
        </div>
      )}
    </Modal>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#111827]">Información Líneas</h1>
          <p className="text-sm text-[#6B7280] mt-1">
            Catálogo técnico. Puedes editar registros (se guardan localmente en este navegador).
          </p>
        </div>
        <div className="text-sm text-[#6B7280]">Registros: {filtered.length}</div>
      </div>

      <div className="bg-white rounded-xl border border-[#E5E7EB] p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <Input
            placeholder="Buscar (clave, número, descripción, conductor, SAP, etc.)"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            icon={<Search className="w-5 h-5" />}
          />
          <div className="hidden md:flex items-center justify-end text-xs text-[#6B7280]">
            Tip: usa el buscador del header o este buscador para filtrar.
          </div>
        </div>
      </div>

      {/* Mobile: cards */}
      <div className="grid grid-cols-1 gap-3 md:hidden">
        {filtered.map((e) => (
          <div
            key={`${e.claveEnlace}-${e.tipoEstructura}-${e.anio ?? 'na'}`}
            className="rounded-xl border border-[#E5E7EB] bg-white p-4 shadow-sm"
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-[#111827]">{e.claveEnlace}</p>
                <p className="text-xs text-[#6B7280] mt-1">{e.descripcion}</p>
              </div>
              <div className="flex items-center gap-2">
                <Badge>{e.tension}</Badge>
                <button
                  type="button"
                  className="p-2 rounded-lg border border-[#E5E7EB] hover:bg-[#F9FAFB]"
                  onClick={() => openEdit(e)}
                  aria-label="Editar"
                >
                  <Pencil className="w-4 h-4 text-[#111827]" />
                </button>
              </div>
            </div>

            <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
              <div><span className="text-[#6B7280]">Área:</span> <span className="text-[#111827] font-medium">{e.area}</span></div>
              <div><span className="text-[#6B7280]">Kms:</span> <span className="text-[#111827] font-medium">{e.kms ?? 'N/A'}</span></div>
              <div><span className="text-[#6B7280]">Conductor:</span> <span className="text-[#111827] font-medium">{e.conductor}</span></div>
              <div><span className="text-[#6B7280]">Conf:</span> <span className="text-[#111827] font-medium">{e.confCond}</span></div>
              <div><span className="text-[#6B7280]">Tip. Estruc:</span> <span className="text-[#111827] font-medium">{e.tipoEstructura}</span></div>
              <div><span className="text-[#6B7280]"># Est:</span> <span className="text-[#111827] font-medium">{e.numEstructuras ?? 'N/A'}</span></div>
              <div><span className="text-[#6B7280]">Año:</span> <span className="text-[#111827] font-medium">{e.anio ?? 'N/A'}</span></div>
              <div><span className="text-[#6B7280]">Cve SAP:</span> <span className="text-[#111827] font-medium">{e.cveSap}</span></div>
              <div><span className="text-[#6B7280]">Brecha:</span> <span className="text-[#111827] font-medium">{e.brecha ?? 'N/A'}</span></div>
              <div><span className="text-[#6B7280]">NC:</span> <span className="text-[#111827] font-medium">{e.nc ?? 'N/A'}</span></div>
              <div><span className="text-[#6B7280]">POB:</span> <span className="text-[#111827] font-medium">{e.pob}</span></div>
              <div><span className="text-[#6B7280]">ENT:</span> <span className="text-[#111827] font-medium">{e.ent}</span></div>
            </div>
          </div>
        ))}
      </div>

      {/* Desktop: table */}
      <div className="hidden md:block">
        <Card>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-[#E5E7EB] text-xs text-[#6B7280]">
                  <th className="text-left py-3 px-4 font-semibold">CLAVE ENLACE</th>
                  <th className="text-left py-3 px-4 font-semibold">DESCRIPCIÓN</th>
                  <th className="text-left py-3 px-4 font-semibold">TENSIÓN</th>
                  <th className="text-left py-3 px-4 font-semibold">KMS</th>
                  <th className="text-left py-3 px-4 font-semibold">CONDUCTOR</th>
                  <th className="text-left py-3 px-4 font-semibold">TIP. ESTRUC</th>
                  <th className="text-left py-3 px-4 font-semibold"># EST</th>
                  <th className="text-left py-3 px-4 font-semibold">AÑO</th>
                  <th className="text-left py-3 px-4 font-semibold">SAP</th>
                  <th className="text-left py-3 px-4 font-semibold">CONF</th>
                  <th className="text-left py-3 px-4 font-semibold">BRECHA</th>
                  <th className="text-left py-3 px-4 font-semibold">NC</th>
                  <th className="text-left py-3 px-4 font-semibold">POB</th>
                  <th className="text-left py-3 px-4 font-semibold">ENT</th>
                  <th className="text-right py-3 px-4 font-semibold">ACCIONES</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((e) => (
                  <tr key={`${e.claveEnlace}-${e.tipoEstructura}-${e.anio ?? 'na'}`} className="border-b border-[#F3F4F6]">
                    <td className="py-3 px-4 font-medium text-[#111827] whitespace-nowrap">{e.claveEnlace}</td>
                    <td className="py-3 px-4 text-[#111827]">{e.descripcion}</td>
                    <td className="py-3 px-4 whitespace-nowrap"><Badge>{e.tension}</Badge></td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.kms ?? 'N/A'}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.conductor}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.tipoEstructura}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.numEstructuras ?? 'N/A'}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.anio ?? 'N/A'}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.cveSap}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.confCond}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.brecha ?? 'N/A'}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.nc ?? 'N/A'}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.pob}</td>
                    <td className="py-3 px-4 whitespace-nowrap">{e.ent}</td>
                    <td className="py-3 px-4 text-right">
                      <button
                        type="button"
                        className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-[#E5E7EB] hover:bg-[#F9FAFB]"
                        onClick={() => openEdit(e)}
                      >
                        <Pencil className="w-4 h-4" />
                        <span className="text-xs font-medium">Editar</span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    </div>
  );
}
