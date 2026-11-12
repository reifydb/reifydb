// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type ButtonHTMLAttributes, forwardRef } from "react";

type ButtonVariant = "primary" | "secondary" | "ghost" | "forest" | "danger" | "destructive" | "link";
type ButtonSize = "sm" | "md" | "lg" | "icon";

const variantStyles: Record<ButtonVariant, string> = {
  primary: "bg-accent-ink text-white font-semibold border-accent-ink hover:bg-primary-dark hover:border-primary-dark",
  secondary: "bg-bg-secondary text-text-primary border-text-primary font-semibold hover:bg-bg-tertiary",
  ghost:
    "text-text-secondary border-transparent hover:bg-bg-tertiary hover:text-text-primary",
  forest: "bg-forest text-white font-semibold border-forest-border hover:brightness-90",
  danger: "bg-status-error/10 text-status-error border-status-error/30 hover:bg-status-error/20",
  destructive: "bg-status-error text-white font-semibold border-status-error hover:brightness-90",
  link:
    "text-primary border-transparent underline-offset-4 hover:underline active:text-primary-dark",
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
        className={`inline-flex items-center justify-center font-display font-semibold uppercase tracking-[0.06em] transition-colors duration-150
          rounded-none border
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
