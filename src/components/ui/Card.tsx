import { ReactNode, HTMLAttributes, memo } from 'react';
import clsx from 'clsx';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
  hover?: boolean;
  clickable?: boolean;
}

const Card = memo(function Card({ children, hover = false, clickable = false, className, ...props }: CardProps) {
  return (
    <div
      className={clsx(
        'bg-white rounded-xl border border-[#E5E7EB] transition-all duration-200',
        {
          'hover:shadow-lg hover:border-[#157A5A]': hover,
          'cursor-pointer': clickable,
        },
        className
      )}
      {...props}
    >
      {children}
    </div>
  );
});

export default Card;
