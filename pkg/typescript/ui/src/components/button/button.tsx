import { type ButtonHTMLAttributes, forwardRef } from "react";

type ButtonVariant = "primary" | "secondary" | "ghost" | "danger" | "destructive" | "link";
type ButtonSize = "sm" | "md" | "lg" | "icon";

const variantStyles: Record<ButtonVariant, string> = {
  primary:
    "bg-primary text-[var(--accent-text,#1a1a1a)] font-semibold border-primary hover:brightness-110 hover:shadow-[0_0_16px_rgba(255,255,255,0.1)]",
  secondary:
    "bg-transparent text-text-secondary border-border-default font-medium hover:border-primary hover:text-primary",
  ghost:
    "text-text-secondary border-transparent hover:bg-white/[0.04] hover:text-text-primary",
  danger:
    "bg-status-error/10 text-status-error border-status-error/30 hover:bg-status-error/20",
  destructive:
    "bg-status-error text-white font-semibold border-status-error hover:bg-status-error/90",
  link:
    "text-primary border-transparent underline-offset-4 hover:underline",
};

const sizeStyles: Record<ButtonSize, string> = {
  sm: "h-8 px-3 text-xs gap-1.5",
  md: "h-9 px-4 text-sm gap-2",
  lg: "h-11 px-6 text-sm gap-2",
  icon: "h-9 w-9 p-0",
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = "primary", size = "md", className = "", children, disabled, ...props }, ref) => {
    return (
      <button
        ref={ref}
        className={`inline-flex items-center justify-center font-medium transition-all duration-200
          rounded-lg border
          focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary
          disabled:pointer-events-none disabled:opacity-50
          ${variantStyles[variant]} ${sizeStyles[size]} ${className}`}
        disabled={disabled}
        {...props}
      >
        {children}
      </button>
    );
  },
);

Button.displayName = "Button";
