// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function GuestBanner({ onSignUp }: { onSignUp: () => void }) {
  return (
    <div className="w-full shrink-0 border-b border-border-light bg-bg-tertiary">
      <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-2.5">
        <p className="font-mono text-xs uppercase tracking-[1.4px] text-text-secondary">
          You are browsing as a guest
          <span className="hidden normal-case tracking-normal opacity-90 sm:inline"> · create an account to keep your pixels</span>
        </p>
        <button type="button" className="btn btn--quiet" onClick={onSignUp}>
          Create account
        </button>
      </div>
    </div>
  )
}
