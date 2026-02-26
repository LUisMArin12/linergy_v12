import { ReactNode, memo } from 'react';
import clsx from 'clsx';

interface ChipProps {
  children: ReactNode;
  selected?: boolean;
  onClick?: () => void;
  icon?: ReactNode;
  className?: string;
}

const Chip = memo(function Chip({ children, selected = false, onClick, icon, className }: ChipProps) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        'inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200',
        'border focus:outline-none focus:ring-2 focus:ring-[#157A5A] focus:ring-offset-1',
        {
          'bg-[#DDF3EA] text-[#0B3D2E] border-[#157A5A]': selected,
          'bg-white text-[#6B7280] border-[#E5E7EB] hover:bg-[#F7FAF8]': !selected,
        },
        className
      )}
    >
      {icon && <span className="text-current">{icon}</span>}
      {children}
    </button>
  );
});

export default Chip;
