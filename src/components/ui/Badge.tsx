import { ReactNode, memo } from 'react';
import clsx from 'clsx';
import { Classification, FaultStatus } from '../../types';

interface BadgeProps {
  children: ReactNode;
  variant?: 'classification' | 'status' | 'default' | 'info' | 'warning';
  classification?: Classification | 'ALTA' | 'MODERADA' | 'BAJA';
  status?: FaultStatus | 'ABIERTA' | 'EN_ATENCION' | 'CERRADA';
  className?: string;
}

const Badge = memo(function Badge({ children, variant = 'default', classification, status, className }: BadgeProps) {
  const getClassificationColor = (c?: string) => {
    if (!c) return 'bg-gray-100 text-gray-800 border-gray-200';
    const normalized = c.toUpperCase();
    switch (normalized) {
      case 'ALTA':
        return 'bg-red-100 text-red-800 border-red-200';
      case 'MODERADA':
        return 'bg-yellow-100 text-yellow-800 border-yellow-200';
      case 'BAJA':
        return 'bg-green-100 text-green-800 border-green-200';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const getStatusColor = (s?: string) => {
    if (!s) return 'bg-gray-100 text-gray-800 border-gray-200';
    const normalized = s.toUpperCase().replace(/\s/g, '_');
    switch (normalized) {
      case 'ABIERTA':
        return 'bg-red-100 text-red-800 border-red-200';
      case 'EN_ATENCIÓN':
      case 'EN_ATENCION':
        return 'bg-blue-100 text-blue-800 border-blue-200';
      case 'CERRADA':
        return 'bg-gray-100 text-gray-800 border-gray-200';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  return (
    <span
      className={clsx(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border',
        {
          [getClassificationColor(classification!)]: variant === 'classification' && classification,
          [getStatusColor(status!)]: variant === 'status' && status,
          'bg-[#DDF3EA] text-[#0B3D2E] border-[#157A5A]': variant === 'default',
          'bg-blue-100 text-blue-800 border-blue-200': variant === 'info',
          'bg-amber-100 text-amber-800 border-amber-200': variant === 'warning',
        },
        className
      )}
    >
      {children}
    </span>
  );
});

export default Badge;
