// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function GuestBanner({ onSignUp }: { onSignUp: () => void }) {
  return (
    <div className="w-full shrink-0 border-b-2 border-[#4b52d6] bg-primary">
      <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-2.5">
        <p className="font-mono text-xs uppercase tracking-[1.4px] text-white">
          You are browsing as a guest
          <span className="hidden normal-case tracking-normal opacity-90 sm:inline"> · create an account to keep your pixels</span>
        </p>
        <button type="button" className="game-button game-button--secondary" onClick={onSignUp}>
          Create account
        </button>
      </div>
    </div>
  )
}
