import { InputHTMLAttributes, ReactNode, forwardRef } from 'react';
import clsx from 'clsx';

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  icon?: ReactNode;
  error?: string;
}

const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ label, icon, error, className, ...props }, ref) => {
    return (
      <div className="w-full">
        {label && (
          <label className="block text-sm font-medium text-[#111827] mb-1.5">
            {label}
          </label>
        )}
        <div className="relative">
          {icon && (
            <div className="absolute left-3 top-1/2 -translate-y-1/2 text-[#6B7280]">
              {icon}
            </div>
          )}
          <input
            ref={ref}
            className={clsx(
              'w-full min-h-[44px] px-4 py-2.5 rounded-lg border border-[#E5E7EB] text-[#111827]',
              'focus:outline-none focus:ring-2 focus:ring-[#157A5A] focus:border-transparent',
              'placeholder:text-[#6B7280] transition-all duration-200',
              'disabled:bg-[#F7FAF8] disabled:cursor-not-allowed',
              {
                'pl-10': icon,
                'border-red-300 focus:ring-red-500': error,
              },
              className
            )}
            {...props}
          />
        </div>
        {error && (
          <p className="mt-1.5 text-sm text-red-600">{error}</p>
        )}
      </div>
    );
  }
);

Input.displayName = 'Input';

export default Input;
