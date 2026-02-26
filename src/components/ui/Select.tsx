import { SelectHTMLAttributes, forwardRef, ReactNode } from 'react';
import clsx from 'clsx';
import { ChevronDown } from 'lucide-react';

interface SelectProps extends SelectHTMLAttributes<HTMLSelectElement> {
  label?: string;
  options?: { value: string | number; label: string }[];
  children?: ReactNode;
}

const Select = forwardRef<HTMLSelectElement, SelectProps>(
  ({ label, options, children, className, ...props }, ref) => {
    return (
      <div className="w-full">
        {label && (
          <label className="block text-sm font-semibold text-[#111827] mb-2">
            {label}
          </label>
        )}
        <div className="relative">
          <select
            ref={ref}
            className={clsx(
              'w-full min-h-[44px] px-4 py-2.5 pr-10 rounded-lg border border-[#E5E7EB] text-[#111827]',
              'focus:outline-none focus:ring-2 focus:ring-[#157A5A] focus:border-transparent',
              'appearance-none bg-white transition-all duration-200',
              'disabled:bg-[#F7FAF8] disabled:cursor-not-allowed',
              className
            )}
            {...props}
          >
            {options ? (
              options.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))
            ) : (
              children
            )}
          </select>
          <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-[#6B7280] pointer-events-none" />
        </div>
      </div>
    );
  }
);

Select.displayName = 'Select';

export default Select;
