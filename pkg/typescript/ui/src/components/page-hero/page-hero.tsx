// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from "react";
import { Container } from "../container/container.js";

export interface PageHeroProps {
  eyebrow?: string;
  title: ReactNode;
  subtitle?: ReactNode;
  scene?: ReactNode;
  children?: ReactNode;
}

export function PageHero({ eyebrow, title, subtitle, scene, children }: PageHeroProps) {
  return (
    <section className="sky">
      <Container className={`pt-[clamp(36px,6vw,72px)] ${scene ? "" : "pb-[clamp(36px,6vw,72px)]"}`}>
        {eyebrow && <p className="eyebrow uppercase mb-3">{eyebrow}</p>}

        <h1 className="max-w-[48rem] text-[clamp(26px,3.4vw,40px)] font-normal leading-[1.08] tracking-[-0.03em] text-text-primary mb-4 text-balance">
          {title}
        </h1>

        {subtitle && <p className="max-w-[48rem] text-text-secondary text-lg">{subtitle}</p>}

        {children && (
          <div className="mt-9 flex flex-col sm:flex-row items-start sm:items-center justify-start gap-4">
            {children}
          </div>
        )}
      </Container>

      {scene && <div className="mt-10 sm:mt-12">{scene}</div>}
    </section>
  );
}
