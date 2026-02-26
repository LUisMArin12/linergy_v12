export type Classification = 'Alta' | 'Moderada' | 'Baja';
export type FaultStatus = 'Abierta' | 'En atención' | 'Cerrada';

export interface Line {
  id: string;
  numero: string;
  rangoKm: string;
  clasificacion: Classification;
}

export interface Structure {
  id: string;
  numero: string;
  lineaId: string;
  lineaNumero: string;
  km: number;
  clasificacion: Classification;
  lat: number;
  lon: number;
}

export interface Fault {
  id: string;
  lineaNumero: string;
  km: number;
  tipo: string;
  estado: FaultStatus;
  fecha: string;
  hora: string;
  lat: number;
  lon: number;
  descripcion: string;
}

