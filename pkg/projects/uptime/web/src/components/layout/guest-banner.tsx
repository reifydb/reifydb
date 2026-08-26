// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link } from '@tanstack/react-router'

export function GuestBanner() {
  return (
    <div className="w-full border-b-2 border-border-default bg-primary">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 py-2.5 sm:px-6">
        <p className="font-mono text-xs uppercase tracking-[1.4px] text-white">
          You are browsing as a guest
          <span className="hidden normal-case tracking-normal opacity-90 sm:inline">
            {' '}
            &mdash; create an account to keep your monitors
          </span>
        </p>
        <Link
          to="/register"
          className="inline-flex h-8 items-center justify-center rounded-none border-2 border-border-default bg-white px-3 font-mono text-xs font-bold uppercase tracking-[1.4px] text-text-primary shadow-[var(--shadow-hard-sm)] transition-none hover:bg-bg-tertiary active:translate-x-[2px] active:translate-y-[2px] active:shadow-none"
        >
          Create account
        </Link>
      </div>
    </div>
  )
}
