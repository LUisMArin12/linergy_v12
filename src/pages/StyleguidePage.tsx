import { useState } from 'react';
import { PlusCircle, Trash2, Search } from 'lucide-react';
import Button from '../components/ui/Button';
import Badge from '../components/ui/Badge';
import Chip from '../components/ui/Chip';
import Card from '../components/ui/Card';
import Input from '../components/ui/Input';
import Select from '../components/ui/Select';
import Modal from '../components/ui/Modal';
import Toast from '../components/ui/Toast';

export default function StyleguidePage() {
  const [showModal, setShowModal] = useState(false);
  const [showToast, setShowToast] = useState(false);

  return (
    <div className="space-y-12 max-w-6xl">
      <div>
        <h1 className="text-3xl font-bold text-[#111827] mb-2">Sistema de Diseño LINERGY</h1>
        <p className="text-[#6B7280]">
          Componentes y estilos base del sistema
        </p>
      </div>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Paleta de Colores</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <div className="h-24 rounded-lg bg-[#F7FAF8] border border-[#E5E7EB] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#F7FAF8</p>
            <p className="text-xs text-[#6B7280]">Fondo</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#FFFFFF] border border-[#E5E7EB] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#FFFFFF</p>
            <p className="text-xs text-[#6B7280]">Blanco</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#157A5A] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#157A5A</p>
            <p className="text-xs text-[#6B7280]">Primario</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#0B3D2E] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#0B3D2E</p>
            <p className="text-xs text-[#6B7280]">Texto Fuerte</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#DDF3EA] border border-[#E5E7EB] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#DDF3EA</p>
            <p className="text-xs text-[#6B7280]">Verde Suave</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#F4C430] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#F4C430</p>
            <p className="text-xs text-[#6B7280]">Acento Amarillo</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#111827] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#111827</p>
            <p className="text-xs text-[#6B7280]">Texto Oscuro</p>
          </div>
          <div>
            <div className="h-24 rounded-lg bg-[#6B7280] mb-2"></div>
            <p className="text-sm font-semibold text-[#111827]">#6B7280</p>
            <p className="text-xs text-[#6B7280]">Texto Secundario</p>
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Botones</h2>
        <div className="space-y-4">
          <div>
            <p className="text-sm font-medium text-[#6B7280] mb-3">Variantes</p>
            <div className="flex flex-wrap gap-3">
              <Button variant="primary">Primario</Button>
              <Button variant="secondary">Secundario</Button>
              <Button variant="ghost">Ghost</Button>
            </div>
          </div>
          <div>
            <p className="text-sm font-medium text-[#6B7280] mb-3">Con íconos</p>
            <div className="flex flex-wrap gap-3">
              <Button variant="primary" icon={<PlusCircle className="w-4 h-4" />}>
                Crear
              </Button>
              <Button variant="secondary" icon={<Search className="w-4 h-4" />}>
                Buscar
              </Button>
              <Button variant="ghost" icon={<Trash2 className="w-4 h-4" />}>
                Eliminar
              </Button>
            </div>
          </div>
          <div>
            <p className="text-sm font-medium text-[#6B7280] mb-3">Tamaños</p>
            <div className="flex flex-wrap items-center gap-3">
              <Button size="sm">Pequeño</Button>
              <Button size="md">Mediano</Button>
              <Button size="lg">Grande</Button>
            </div>
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Badges</h2>
        <div className="space-y-4">
          <div>
            <p className="text-sm font-medium text-[#6B7280] mb-3">Clasificación</p>
            <div className="flex flex-wrap gap-2">
              <Badge variant="classification" classification="Alta">Alta</Badge>
              <Badge variant="classification" classification="Moderada">Moderada</Badge>
              <Badge variant="classification" classification="Baja">Baja</Badge>
            </div>
          </div>
          <div>
            <p className="text-sm font-medium text-[#6B7280] mb-3">Estado Fallas</p>
            <div className="flex flex-wrap gap-2">
              <Badge variant="status" status="Abierta">Abierta</Badge>
              <Badge variant="status" status="En atención">En atención</Badge>
              <Badge variant="status" status="Cerrada">Cerrada</Badge>
            </div>
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Chips</h2>
        <div className="flex flex-wrap gap-2">
          <Chip selected>Seleccionado</Chip>
          <Chip>No seleccionado</Chip>
          <Chip icon={<PlusCircle className="w-4 h-4" />}>Con ícono</Chip>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Inputs y Selects</h2>
        <div className="grid gap-4 max-w-md">
          <Input label="Input básico" placeholder="Escribe algo..." />
          <Input
            label="Input con ícono"
            icon={<Search className="w-4 h-4" />}
            placeholder="Buscar..."
          />
          <Input
            label="Input con error"
            error="Este campo es requerido"
            placeholder="Campo con error"
          />
          <Select
            label="Select básico"
            options={[
              { value: '1', label: 'Opción 1' },
              { value: '2', label: 'Opción 2' },
              { value: '3', label: 'Opción 3' },
            ]}
          />
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Cards</h2>
        <div className="grid md:grid-cols-2 gap-4">
          <Card className="p-6">
            <h3 className="font-semibold text-[#111827] mb-2">Card básica</h3>
            <p className="text-sm text-[#6B7280]">
              Esta es una card básica sin efectos especiales
            </p>
          </Card>
          <Card hover className="p-6">
            <h3 className="font-semibold text-[#111827] mb-2">Card con hover</h3>
            <p className="text-sm text-[#6B7280]">
              Pasa el cursor sobre esta card para ver el efecto
            </p>
          </Card>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Modal</h2>
        <Button onClick={() => setShowModal(true)}>Abrir Modal</Button>
        <Modal isOpen={showModal} onClose={() => setShowModal(false)} title="Ejemplo de Modal">
          <div className="space-y-4">
            <p className="text-[#6B7280]">
              Este es un ejemplo de modal con animación de entrada y salida.
            </p>
            <Input label="Campo de ejemplo" placeholder="Escribe algo..." />
            <div className="flex gap-3 pt-4">
              <Button variant="primary" className="flex-1">
                Guardar
              </Button>
              <Button variant="secondary" onClick={() => setShowModal(false)} className="flex-1">
                Cancelar
              </Button>
            </div>
          </div>
        </Modal>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Toast</h2>
        <div className="flex gap-3">
          <Button onClick={() => setShowToast(true)}>Mostrar Toast</Button>
        </div>
        <Toast
          message="Operación completada con éxito"
          type="success"
          isVisible={showToast}
          onClose={() => setShowToast(false)}
        />
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Tabla</h2>
        <Card className="overflow-hidden">
          <table className="w-full">
            <thead className="bg-[#F7FAF8]">
              <tr>
                <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">Columna 1</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">Columna 2</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-[#111827]">Columna 3</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-t border-[#E5E7EB]">
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 1</td>
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 2</td>
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 3</td>
              </tr>
              <tr className="border-t border-[#E5E7EB]">
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 4</td>
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 5</td>
                <td className="py-3 px-4 text-sm text-[#111827]">Dato 6</td>
              </tr>
            </tbody>
          </table>
        </Card>
      </section>

      <section>
        <h2 className="text-2xl font-bold text-[#111827] mb-6">Estados</h2>
        <div className="grid md:grid-cols-2 gap-6">
          <Card className="p-8 text-center">
            <div className="w-12 h-12 bg-[#F7FAF8] rounded-full flex items-center justify-center mx-auto mb-3">
              <Search className="w-6 h-6 text-[#6B7280]" />
            </div>
            <h3 className="font-semibold text-[#111827] mb-1">Empty State</h3>
            <p className="text-sm text-[#6B7280]">No hay datos para mostrar</p>
          </Card>

          <Card className="p-8 text-center border-red-200 bg-red-50">
            <h3 className="font-semibold text-red-800 mb-1">Error State</h3>
            <p className="text-sm text-red-600">Ocurrió un error al cargar los datos</p>
            <Button variant="secondary" size="sm" className="mt-4">
              Reintentar
            </Button>
          </Card>
        </div>
      </section>
    </div>
  );
}
