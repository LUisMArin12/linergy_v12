import { ButtonHTMLAttributes, ReactNode } from 'react';
import clsx from 'clsx';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost' | 'danger';
  size?: 'sm' | 'md' | 'lg';
  icon?: ReactNode;
  children: ReactNode;
}

export default function Button({
  variant = 'primary',
  size = 'md',
  icon,
  children,
  className,
  ...props
}: ButtonProps) {
  return (
    <button
      className={clsx(
        'inline-flex items-center justify-center gap-2 rounded-lg font-medium transition-all duration-200',
        'focus:outline-none focus:ring-2 focus:ring-offset-2',
        'disabled:opacity-50 disabled:cursor-not-allowed',
        'select-none',
        {
          'bg-[#157A5A] text-white hover:bg-[#0B3D2E] focus:ring-[#157A5A] shadow-sm hover:shadow-md': variant === 'primary',
          'bg-white text-[#111827] border border-[#E5E7EB] hover:bg-[#F7FAF8] focus:ring-[#157A5A]': variant === 'secondary',
          'text-[#111827] hover:bg-[#F7FAF8] focus:ring-[#157A5A]': variant === 'ghost',
          'bg-rose-600 text-white hover:bg-rose-700 focus:ring-rose-600 shadow-sm hover:shadow-md': variant === 'danger',
          // Touch-friendly (>=44px)
          'px-3 py-2 text-sm min-h-[40px]': size === 'sm',
          'px-4 py-2.5 text-sm min-h-[44px]': size === 'md',
          'px-6 py-3 text-base min-h-[48px]': size === 'lg',
        },
        className
      )}
      {...props}
    >
      {icon && <span>{icon}</span>}
      {children}
    </button>
  );
}
