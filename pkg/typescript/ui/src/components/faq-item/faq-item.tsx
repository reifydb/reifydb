// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState, type ReactNode } from "react";

export interface FaqItemProps {
  question: string;
  answer: string;
  portrait?: ReactNode;
}

const CHARACTERS_PER_TICK = 2;
const TICK_MS = 25;

export function FaqItem({ question, answer, portrait }: FaqItemProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [shown, setShown] = useState(0);
  const done = shown >= answer.length;

  useEffect(() => {
    if (!isOpen || done) return;
    const timer = window.setTimeout(() => setShown((n) => Math.min(answer.length, n + CHARACTERS_PER_TICK)), TICK_MS);
    return () => window.clearTimeout(timer);
  }, [isOpen, done, shown, answer.length]);

  return (
    <div className="faq-item">
      <button
        onClick={() => {
          setIsOpen(!isOpen);
          setShown(0);
        }}
        className="faq-item__question"
        aria-expanded={isOpen}
      >
        <svg
          viewBox="0 0 4 7"
          className={`h-3.5 w-2 shrink-0 text-accent-ink transition-transform ${isOpen ? "rotate-90" : ""}`}
          shapeRendering="crispEdges"
          fill="currentColor"
          aria-hidden="true"
        >
          <rect x="0" y="0" width="1" height="7" />
          <rect x="1" y="1" width="1" height="5" />
          <rect x="2" y="2" width="1" height="3" />
          <rect x="3" y="3" width="1" height="1" />
        </svg>
        <span>{question}</span>
      </button>
      {isOpen && (
        <div className="faq-item__answer" onClick={() => setShown(answer.length)}>
          {portrait && <div className="faq-item__portrait">{portrait}</div>}
          <p className="text-text-secondary text-sm leading-relaxed">
            <span className="sr-only">{answer}</span>
            <span aria-hidden="true">
              {answer.slice(0, shown)}
              <span className="invisible">{answer.slice(shown)}</span>
              {done && <span className="faq-item__next">▼</span>}
            </span>
          </p>
        </div>
      )}
    </div>
  );
}
