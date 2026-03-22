import type { ReactNode, ButtonHTMLAttributes } from 'react'
import { Link } from 'react-router-dom'
import { cn } from '@/lib'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
  href?: string
  children: ReactNode
}

const variantStyles = {
  primary: cn(
    'bg-bg-primary border border-primary text-primary',
    'hover:bg-primary hover:text-bg-primary',
    'transition-all duration-200'
  ),
  secondary: cn(
    'bg-bg-primary border-2 border-dashed border-black/25 text-text-secondary',
    'hover:text-text-primary hover:border-black/40',
    'transition-all duration-200'
  ),
  ghost: cn(
    'text-text-secondary',
    'underline underline-offset-4 decoration-text-muted',
    'hover:text-text-primary hover:decoration-primary',
    'active:opacity-80'
  ),
}

const sizeStyles = {
  sm: 'px-3 py-1.5 text-sm',
  md: 'px-5 py-2.5 text-sm',
  lg: 'px-6 py-3 text-base',
}

function wrapChildren(variant: string, children: ReactNode) {
  if (variant === 'primary') return <>[&gt; {children}]</>
  if (variant === 'secondary') return <>[{children}]</>
  return children
}

export function Button({
  variant = 'primary',
  size = 'md',
  href,
  children,
  className,
  ...props
}: ButtonProps) {
  const baseStyles = cn(
    'inline-flex items-center justify-center gap-2',
    'font-mono font-medium uppercase tracking-wider transition-all duration-200',
    'focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 focus:ring-offset-bg-primary',
    variantStyles[variant],
    sizeStyles[size],
    className
  )

  const wrapped = wrapChildren(variant, children)

  if (href) {
    if (href.startsWith('/')) {
      return (
        <Link to={href} className={baseStyles}>
          {wrapped}
        </Link>
      )
    }
    return (
      <a href={href} className={baseStyles} target="_blank" rel="noopener noreferrer">
        {wrapped}
      </a>
    )
  }

  return (
    <button className={baseStyles} {...props}>
      {wrapped}
    </button>
  )
}
