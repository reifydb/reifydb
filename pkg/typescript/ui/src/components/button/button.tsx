// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { type ButtonHTMLAttributes, forwardRef } from "react";

type ButtonVariant = "primary" | "secondary" | "ghost" | "forest" | "danger" | "destructive" | "link";
type ButtonSize = "sm" | "md" | "lg" | "icon";

const variantStyles: Record<ButtonVariant, string> = {
  primary:
    "bg-primary text-[#141414] font-semibold border-primary hover:brightness-110",
  secondary:
    "bg-[#141414] text-white border-[#141414] font-medium hover:bg-bg-elevated hover:text-primary",
  ghost:
    "text-white border-border-ghost hover:text-primary",
  forest:
    "bg-forest text-white font-semibold border-forest-border hover:brightness-110",
  danger:
    "bg-status-error/10 text-status-error border-status-error/30 hover:bg-status-error/20",
  destructive:
    "bg-status-error text-white font-semibold border-status-error hover:bg-status-error/90",
  link:
    "text-primary border-transparent underline-offset-4 hover:underline active:text-primary-light",
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
          rounded-[4px] border
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
