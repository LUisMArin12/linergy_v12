import { useState, DragEvent } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { UploadCloud, CheckCircle, AlertTriangle, XCircle } from 'lucide-react';
import Button from '../components/ui/Button';
import Card from '../components/ui/Card';
import { importKMZ } from '../lib/supabase';
import { useToast } from '../contexts/ToastContext';

interface ImportResult {
  lineas_created: number;
  tramos_inserted: number;
  estructuras_inserted: number;
  lineas_finalized: number;
  errores: string[];
  warnings: string[];
}

export default function AdminImportPage() {
  const queryClient = useQueryClient();
  const { showToast } = useToast();
  const [isDragging, setIsDragging] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<ImportResult | null>(null);

  const importMutation = useMutation({
    mutationFn: async (file: File) => {
      return await importKMZ(file);
    },
    onSuccess: (data) => {
      setResult(data);
      queryClient.invalidateQueries({ queryKey: ['lineas'] });
      queryClient.invalidateQueries({ queryKey: ['estructuras'] });

      const total = data.lineas_created + data.estructuras_inserted;
      if (data.errores.length > 0) {
        showToast(`KMZ importado con ${data.errores.length} errores. ${total} elementos procesados.`, 'info');
      } else {
        showToast(`KMZ importado exitosamente. ${total} elementos procesados.`, 'success');
      }
    },
    onError: (error) => {
      const errorMessage = error instanceof Error ? error.message : 'Error desconocido';
      showToast(`Error al importar KMZ: ${errorMessage}`, 'error');
    },
  });

  const handleDragOver = (e: DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleDrop = (e: DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile && (droppedFile.name.endsWith('.kmz') || droppedFile.name.endsWith('.kml'))) {
      setFile(droppedFile);
      setResult(null);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0];
    if (selectedFile) {
      setFile(selectedFile);
      setResult(null);
    }
  };

  const handleImport = () => {
    if (file) {
      importMutation.mutate(file);
    }
  };

  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <h1 className="text-2xl font-bold text-[#111827]">Importar KMZ</h1>
        <p className="text-sm text-[#6B7280] mt-1">
          Importa archivos KMZ con información de líneas y estructuras
        </p>
      </div>

      <Card className="p-8">
        <div
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
          className={`
            border-2 border-dashed rounded-xl p-12 text-center transition-all duration-200
            ${
              isDragging
                ? 'border-[#157A5A] bg-[#DDF3EA]'
                : 'border-[#E5E7EB] hover:border-[#157A5A] hover:bg-[#F7FAF8]'
            }
          `}
        >
          <div className="flex flex-col items-center gap-4">
            <div className="w-16 h-16 bg-[#DDF3EA] rounded-full flex items-center justify-center">
              <UploadCloud className="w-8 h-8 text-[#157A5A]" />
            </div>
            <div>
              <p className="text-lg font-semibold text-[#111827]">
                {file ? file.name : 'Arrastra tu archivo KMZ aquí'}
              </p>
              <p className="text-sm text-[#6B7280] mt-1">o haz clic para seleccionar</p>
            </div>
            <input
              type="file"
              accept=".kmz,.kml"
              className="hidden"
              id="file-input"
              onChange={handleFileChange}
            />
            <label htmlFor="file-input" className="cursor-pointer">
              <span className="inline-flex items-center justify-center gap-2 rounded-lg font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 bg-white text-[#111827] border border-[#E5E7EB] hover:bg-[#F7FAF8] focus:ring-[#157A5A] px-4 py-2 text-sm">
                Seleccionar archivo
              </span>
            </label>
          </div>
        </div>

        {file && !importMutation.isPending && !result && (
          <div className="mt-6 flex justify-end">
            <Button variant="primary" onClick={handleImport}>
              Importar archivo
            </Button>
          </div>
        )}

        {importMutation.isPending && (
          <div className="mt-6">
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
              <p className="text-sm text-blue-600 font-medium">Procesando archivo...</p>
            </div>
          </div>
        )}

        {importMutation.isError && (
          <div className="mt-6">
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="font-semibold text-red-800">Error al importar archivo</p>
                  <p className="text-sm text-red-600 mt-1">
                    {importMutation.error instanceof Error
                      ? importMutation.error.message
                      : 'Error desconocido'}
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </Card>

      {result && (
        <div className="space-y-4">
          <h2 className="text-lg font-semibold text-[#111827]">Resultado de importación</h2>

          <div className="grid grid-cols-4 gap-4">
            <Card className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#DDF3EA] rounded-lg flex items-center justify-center">
                  <CheckCircle className="w-5 h-5 text-[#157A5A]" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-[#111827]">{result.lineas_created}</p>
                  <p className="text-xs text-[#6B7280]">Líneas creadas</p>
                </div>
              </div>
            </Card>

            <Card className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#DDF3EA] rounded-lg flex items-center justify-center">
                  <CheckCircle className="w-5 h-5 text-[#157A5A]" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-[#111827]">
                    {result.tramos_inserted}
                  </p>
                  <p className="text-xs text-[#6B7280]">Tramos insertados</p>
                </div>
              </div>
            </Card>

            <Card className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#DDF3EA] rounded-lg flex items-center justify-center">
                  <CheckCircle className="w-5 h-5 text-[#157A5A]" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-[#111827]">
                    {result.estructuras_inserted}
                  </p>
                  <p className="text-xs text-[#6B7280]">Estructuras</p>
                </div>
              </div>
            </Card>

            <Card className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#DDF3EA] rounded-lg flex items-center justify-center">
                  <CheckCircle className="w-5 h-5 text-[#157A5A]" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-[#111827]">
                    {result.lineas_finalized}
                  </p>
                  <p className="text-xs text-[#6B7280]">Finalizadas</p>
                </div>
              </div>
            </Card>
          </div>

          {result.errores.length > 0 && (
            <Card className="p-4 border-yellow-200 bg-yellow-50">
              <div className="flex items-start gap-3">
                <AlertTriangle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="font-semibold text-[#111827] mb-2">
                    Se encontraron {result.errores.length} errores
                  </p>
                  <ul className="space-y-2 max-h-96 overflow-y-auto">
                    {result.errores.map((error, idx) => (
                      <li key={idx} className="text-sm text-[#6B7280] bg-white rounded p-2">
                        • {error}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </Card>
          )}

          {result.warnings.length > 0 && (
            <Card className="p-4 border-yellow-200 bg-yellow-50">
              <div className="flex items-start gap-3">
                <AlertTriangle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="font-semibold text-[#111827] mb-2">Advertencias</p>
                  <ul className="space-y-1">
                    {result.warnings.map((warning, idx) => (
                      <li key={idx} className="text-sm text-[#6B7280]">
                        • {warning}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </Card>
          )}

          <div className="flex justify-end pt-4">
            <Button
              variant="secondary"
              onClick={() => {
                setFile(null);
                setResult(null);
                importMutation.reset();
              }}
            >
              Importar otro archivo
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
