// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, forwardRef, type ReactNode, type ButtonHTMLAttributes } from "react";

export interface DialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  children: ReactNode;
}

export function Dialog({ open, onOpenChange, children }: DialogProps) {
  useEffect(() => {
    if (open) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "unset";
    }
    return () => {
      document.body.style.overflow = "unset";
    };
  }, [open]);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") onOpenChange(false);
    };
    if (open) {
      document.addEventListener("keydown", handleEscape);
    }
    return () => {
      document.removeEventListener("keydown", handleEscape);
    };
  }, [open, onOpenChange]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={() => onOpenChange(false)} />
      <div className="relative z-50">{children}</div>
    </div>
  );
}

export interface DialogContentProps {
  className?: string;
  children: ReactNode;
}

export const DialogContent = forwardRef<HTMLDivElement, DialogContentProps>(
  ({ className = "", children }, ref) => (
    <div
      ref={ref}
      className={`relative w-full max-w-lg mx-4 bg-bg-secondary border border-border-default rounded-[var(--radius-md)] shadow-xl ${className}`}
      onClick={(e) => e.stopPropagation()}
    >
      {children}
    </div>
  ),
);
DialogContent.displayName = "DialogContent";

export function DialogHeader({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`flex flex-col gap-1.5 p-6 border-b border-border-light ${className}`}>
      {children}
    </div>
  );
}

export const DialogTitle = forwardRef<HTMLHeadingElement, { children: ReactNode; className?: string }>(
  ({ children, className = "" }, ref) => (
    <h2 ref={ref} className={`text-base font-semibold text-text-primary ${className}`}>
      {children}
    </h2>
  ),
);
DialogTitle.displayName = "DialogTitle";

export const DialogDescription = forwardRef<HTMLParagraphElement, { children: ReactNode; className?: string }>(
  ({ children, className = "" }, ref) => (
    <p ref={ref} className={`text-sm text-text-muted ${className}`}>
      {children}
    </p>
  ),
);
DialogDescription.displayName = "DialogDescription";

export const DialogClose = forwardRef<HTMLButtonElement, ButtonHTMLAttributes<HTMLButtonElement>>(
  ({ className = "", ...props }, ref) => (
    <button
      ref={ref}
      className={`absolute right-4 top-4 h-6 w-6 flex items-center justify-center text-text-muted hover:text-text-primary transition-colors ${className}`}
      aria-label="Close"
      {...props}
    >
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </svg>
    </button>
  ),
);
DialogClose.displayName = "DialogClose";

export function DialogFooter({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`flex justify-end gap-3 p-6 border-t border-border-light ${className}`}>
      {children}
    </div>
  );
}
